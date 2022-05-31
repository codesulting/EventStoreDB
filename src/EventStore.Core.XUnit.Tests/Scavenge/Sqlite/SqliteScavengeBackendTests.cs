using System;
using System.Diagnostics;
using System.Linq;
using EventStore.Core.TransactionLog.Scavenging;
using EventStore.Core.TransactionLog.Scavenging.Sqlite;
using Xunit;
using Xunit.Abstractions;

namespace EventStore.Core.XUnit.Tests.Scavenge.Sqlite {
	public class SqliteScavengeBackendTests : DirectoryPerTest<SqliteScavengeBackendTests> {
		private readonly ITestOutputHelper _testOutputHelper;

		public SqliteScavengeBackendTests(ITestOutputHelper testOutputHelper) {
			_testOutputHelper = testOutputHelper;
		}

		[Fact]
		public void should_successfully_enable_features_on_initialization() {
			using (var sut = new SqliteScavengeBackend<string>()) {
				var result = Record.Exception(() => sut.Initialize(Fixture.Directory));
				Assert.Null(result);
			}
		}
		
		[Fact]
		public void should_restore_previous_state_on_rollback() {
			using (var sut = new SqliteScavengeBackend<string>()) {
				sut.Initialize(Fixture.Directory);

				// Setup
				var tx1 = sut.Begin();
				sut.ChunkWeights[0] = 1f;
				sut.ChunkWeights[1] = 1f;
				sut.ChunkWeights[2] = 1f;
				
				sut.Hashes[0] = "hash-one";
				sut.Hashes[1] = "hash-two";
				sut.Hashes[2] = "hash-three";
				sut.Commit(tx1);
				
				// Rollback
				var tx2 = sut.Begin();
				sut.ChunkWeights[3] = 2f;
				sut.ChunkWeights[4] = 2f;
				
				sut.Hashes[3] = "hash-four";
				sut.Hashes[4] = "hash-five";
				
				Assert.Equal(5, sut.ChunkWeights.AllRecords().Count());
				Assert.Equal(5, sut.Hashes.AllRecords().Count());
				
				sut.Rollback(tx2);
				
				Assert.Equal(3, sut.ChunkWeights.AllRecords().Count());
				Assert.Equal(3, sut.Hashes.AllRecords().Count());
			}
		}
		
		[Fact, Trait("Category", "LongRunning")]
		public void test_memory_usage() {
			const ulong streamCount = 1_000_000;
			const int chunkCount = 100;
			const int collisionStorageCount = 5;
			const int cacheSizeInBytes = 4 * 1024 * 1024;
			
			using (var sut = new SqliteScavengeBackend<string>(cacheSizeInBytes)) {
				sut.Initialize(Fixture.Directory);

				var stopwatch = new Stopwatch();
				stopwatch.Start();

				var transaction = sut.Begin();
				for (ulong i = 0; i < streamCount; i++) {
					var streamId = Guid.NewGuid().ToString();
					sut.Hashes[i] = streamId;
					sut.MetaStorage[i] = new MetastreamData(false, DiscardPoint.KeepAll);
					sut.OriginalStorage[i] = new OriginalStreamData() {
						Status = CalculationStatus.Active,
						DiscardPoint = DiscardPoint.KeepAll,
						IsTombstoned = false,
						MaxAge = TimeSpan.FromHours(1),
						MaxCount = 10,
						MaybeDiscardPoint = DiscardPoint.KeepAll,
						TruncateBefore = 15
					};
				}

				for (int i = 0; i < chunkCount; i++) {
					sut.ChunkWeights[i] = 0.5f;
					sut.ChunkTimeStampRanges[i] = new ChunkTimeStampRange(DateTime.Today, DateTime.Today.AddHours(1*i));
				}

				for (int i = 0; i < collisionStorageCount; i++) {
					sut.CollisionStorage[$"stream-collision-{i}"] = Unit.Instance;
					sut.MetaCollisionStorage[$"meta-stream-collision-{i}"] = new MetastreamData(isTombstoned: true, DiscardPoint.KeepAll);
					sut.OriginalCollisionStorage[$"stream-collision-{i}"] = new OriginalStreamData() {
						Status = CalculationStatus.Archived,
						DiscardPoint = DiscardPoint.KeepAll,
						IsTombstoned = false,
						MaxAge = TimeSpan.FromHours(1),
						MaxCount = 10,
						MaybeDiscardPoint = DiscardPoint.KeepAll,
						TruncateBefore = 15
					};
				}
				
				sut.Commit(transaction);
				stopwatch.Stop();
				
				var stats = sut.GetStats();
				_testOutputHelper.WriteLine(
					$"SQLite Memory Usage: {stats.MemoryUsage}, " +
					$"Db Size: {stats.DatabaseSize}, " +
					$"Cache Size: {stats.CacheSize} for {streamCount} streams in {stopwatch.Elapsed}");
			}
		}
	}
}
