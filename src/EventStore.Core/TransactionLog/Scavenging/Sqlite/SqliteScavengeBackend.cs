using System;
using System.IO;
using Microsoft.Data.Sqlite;
using SQLitePCL;

namespace EventStore.Core.TransactionLog.Scavenging.Sqlite {
	public class SqliteScavengeBackend<TStreamId> : ITransactionFactory<SqliteTransaction>, IDisposable {
		private const string DbFileName = "scavenging.db";
		private const string ExpectedJournalMode = "wal";
		private const int ExpectedSynchronousValue = 1; // Normal
		private const int DefaultSqliteCacheSize = 2 * 1024 * 1024;
		private readonly int _cacheSizeInBytes;
		private SqliteConnection _connection;
		private SqliteBackend _sqliteBackend;

		public IScavengeMap<TStreamId, Unit> CollisionStorage { get; private set; }
		public IScavengeMap<ulong,TStreamId> Hashes { get; private set; }
		public IMetastreamScavengeMap<ulong> MetaStorage { get; private set; }
		public IMetastreamScavengeMap<TStreamId> MetaCollisionStorage { get; private set; }
		public IOriginalStreamScavengeMap<ulong> OriginalStorage { get; private set; }
		public IOriginalStreamScavengeMap<TStreamId> OriginalCollisionStorage { get; private set; }
		public IScavengeMap<Unit,ScavengeCheckpoint> CheckpointStorage { get; private set; }
		public IScavengeMap<int,ChunkTimeStampRange> ChunkTimeStampRanges { get; private set; }
		public IChunkWeightScavengeMap ChunkWeights { get; private set; }
		private ISqliteScavengeBackend[] AllMaps { get; set; }

		public SqliteScavengeBackend(int cacheSizeInBytes=DefaultSqliteCacheSize) {
			_cacheSizeInBytes = Math.Abs(cacheSizeInBytes);
		}

		public void Initialize(string dir = ".") {
			OpenDbConnection(dir);
			ConfigureFeatures();

			var collisionStorage = new SqliteFixedStructScavengeMap<TStreamId, Unit>("CollisionStorageMap");
			CollisionStorage = collisionStorage;

			var hashes = new SqliteScavengeMap<ulong, TStreamId>("HashesMap");
			Hashes = hashes;

			var metaStorage = new SqliteMetastreamScavengeMap<ulong>("MetaStorageMap");
			MetaStorage = metaStorage;
			
			var metaCollisionStorage = new SqliteMetastreamScavengeMap<TStreamId>("MetaCollisionMap");
			MetaCollisionStorage = metaCollisionStorage;
			
			var originalStorage = new SqliteOriginalStreamScavengeMap<ulong>("OriginalStreamStorageMap");
			OriginalStorage = originalStorage;
			
			var originalCollisionStorage = new SqliteOriginalStreamScavengeMap<TStreamId>("OriginalStreamCollisionStorageMap");
			OriginalCollisionStorage = originalCollisionStorage;
			
			var checkpointStorage = new SqliteScavengeCheckpointMap<TStreamId>();
			CheckpointStorage = checkpointStorage;
			
			var chunkTimeStampRanges = new SqliteFixedStructScavengeMap<int, ChunkTimeStampRange>("ChunkTimeStampRangeMap");
			ChunkTimeStampRanges = chunkTimeStampRanges;
			
			var chunkWeights = new SqliteChunkWeightScavengeMap();
			ChunkWeights = chunkWeights;

			AllMaps = new ISqliteScavengeBackend[] { collisionStorage, hashes, metaStorage, metaCollisionStorage,
				originalStorage, originalCollisionStorage, checkpointStorage, chunkTimeStampRanges, chunkWeights };

			var transaction = Begin();
			
			foreach (var map in AllMaps) {
				map.Initialize(_sqliteBackend);
			}
			
			Commit(transaction);
		}

		private void OpenDbConnection(string dir) {
			Directory.CreateDirectory(dir);

			var connectionStringBuilder = new SqliteConnectionStringBuilder();
			connectionStringBuilder.DataSource = Path.Combine(dir, DbFileName);
			// We use a single connection, no pooling needed. When enabled, keeps the files locked on connection close.
			connectionStringBuilder.Pooling = false;
			_connection = new SqliteConnection(connectionStringBuilder.ConnectionString);
			_connection.Open();

			_sqliteBackend = new SqliteBackend(_connection);
		}

		private void ConfigureFeatures() {
			SetPragmaValue("journal_mode", ExpectedJournalMode);
			var journalMode = GetPragmaValue("journal_mode");
			if (journalMode.ToLower() != ExpectedJournalMode) {
				throw new Exception($"Failed to configure journal mode, unexpected value: {journalMode}");
			}
			
			SetPragmaValue("synchronous", ExpectedSynchronousValue.ToString());
			var synchronousMode = int.Parse(GetPragmaValue("synchronous"));
			if (synchronousMode != ExpectedSynchronousValue) {
				throw new Exception($"Failed to configure synchronous mode, unexpected value: {synchronousMode}");
			}

			// cache size in kibi bytes is passed as a negative value, otherwise it's amount of pages
			var kiloBytesToKibiBytes = 1000f / 1024f;
			var cacheSizeInKibiBytes = (int)(_cacheSizeInBytes / 1024f * kiloBytesToKibiBytes);
			var defaultCacheSizeInKibiBytes = (int)(DefaultSqliteCacheSize / 1024f * kiloBytesToKibiBytes);
			var cacheSize = Math.Max(cacheSizeInKibiBytes, defaultCacheSizeInKibiBytes);
			SetPragmaValue("cache_size", (-1 * cacheSize).ToString());
			var currentCacheSize = int.Parse(GetPragmaValue("cache_size"));
			if (-1 * currentCacheSize != cacheSize) {
				throw new Exception($"Failed to configure cache size, unexpected value: {currentCacheSize}");
			}
		}

		public Stats GetStats() {
			var databaseSize = int.Parse(GetPragmaValue("page_size")) * int.Parse(GetPragmaValue("page_count"));
			var kibiBytesToKiloBytes = 1024f / 1000f;
			var cacheSizeInKibiBytes = -1 * int.Parse(GetPragmaValue("cache_size"));
			var cacheSizeInKiloBytes = (int)(cacheSizeInKibiBytes * kibiBytesToKiloBytes);
			var cacheSizeInBytes = cacheSizeInKiloBytes * 1024;
			
			return new Stats(raw.sqlite3_memory_used(), databaseSize, cacheSizeInBytes);
		}

		private void SetPragmaValue(string name, string value) {
			using (var cmd = _connection.CreateCommand()) {
				cmd.CommandText = $"PRAGMA {name}={value}";
				cmd.ExecuteNonQuery();
			}
		}
		
		private string GetPragmaValue(string name) {
			var cmd = _connection.CreateCommand();
			cmd.CommandText = "PRAGMA " + name;
			var result = cmd.ExecuteScalar();
			
			if (result != null) {
				return result.ToString();
			}

			throw new Exception("Unexpected pragma result!");
		}

		public SqliteTransaction Begin() {
			if (_connection == null) {
				throw new InvalidOperationException("Cannot start a scavenge state transaction without an open connection");
			}

			return _sqliteBackend.BeginTransaction();
		}

		public void Rollback(SqliteTransaction transaction) {
			if (transaction == null) {
				throw new InvalidOperationException("Cannot rollback a scavenge state transaction without an active transaction");
			}

			transaction.Rollback();
			transaction.Dispose();
			_sqliteBackend.ClearTransaction();
		}

		public void Commit(SqliteTransaction transaction) {
			if (transaction == null) {
				throw new InvalidOperationException("Cannot commit a scavenge state transaction without an active transaction");
			}
			
			transaction.Commit();
			transaction.Dispose();
			_sqliteBackend.ClearTransaction();
		}

		public void Dispose() {
			_connection?.Dispose();
		}
		
		public class Stats {
			public Stats(long memoryUsage, int databaseSize, int cacheSize) {
				MemoryUsage = memoryUsage;
				DatabaseSize = databaseSize;
				CacheSize = cacheSize;
			}

			public long MemoryUsage { get; }
			public long DatabaseSize { get; }
			public long CacheSize { get; }
		}
	}
}
