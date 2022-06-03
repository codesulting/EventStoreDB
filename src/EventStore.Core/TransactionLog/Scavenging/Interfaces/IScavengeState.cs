using System;
using System.Collections.Generic;
using EventStore.Core.Data;

namespace EventStore.Core.TransactionLog.Scavenging {
	public interface IScavengeState<TStreamId> :
		IScavengeStateForAccumulator<TStreamId>,
		IScavengeStateForCalculator<TStreamId>,
		IScavengeStateForIndexExecutor<TStreamId>,
		IScavengeStateForChunkMerger,
		IScavengeStateForChunkExecutor<TStreamId>,
		IScavengeStateForCleaner {

		bool TryGetCheckpoint(out ScavengeCheckpoint checkpoint);

		IEnumerable<TStreamId> AllCollisions();
	}

	// all the components use these
	public interface IScavengeStateCommon {
		// begin a transaction, returns the started transaction so that it can be
		// committed or rolled back
		ITransactionCompleter BeginTransaction();
	}

	// abstraction to allow the components to commit/rollback
	public interface ITransactionCompleter {
		void Rollback();
		void Commit(ScavengeCheckpoint checkpoint);
	}

	public interface ITransactionManager : ITransactionCompleter{
		void Begin();
		void RegisterOnRollback(Action onRollback);
	}

	// abstraction for the backing store. memory, sqlite etc.
	public interface ITransactionFactory<TTransaction> {
		TTransaction Begin();
		void Rollback(TTransaction transaction);
		void Commit(TTransaction transaction);
	}

	public interface IScavengeStateForAccumulator<TStreamId> :
		IScavengeStateCommon,
		IIncreaseChunkWeights {

		// call this for each record as we accumulate through the log so that we can spot every hash
		// collision to save ourselves work later.
		// this affects multiple parts of the scavengestate and must be called within a transaction so
		// that its effect is atomic
		void DetectCollisions(TStreamId streamId);

		void SetMetastreamDiscardPoint(TStreamId metastreamId, DiscardPoint discardPoint);

		// for when the _original_ stream is tombstoned, the metadata stream can be removed entirely.
		void SetMetastreamTombstone(TStreamId metastreamId);

		void SetOriginalStreamMetadata(TStreamId originalStreamId, StreamMetadata metadata);

		void SetOriginalStreamTombstone(TStreamId originalStreamId);

		void SetChunkTimeStampRange(int logicalChunkNumber, ChunkTimeStampRange range);

		StreamHandle<TStreamId> GetStreamHandle(TStreamId streamId);
	}

	public interface IScavengeStateForCalculatorReadOnly<TStreamId> : IScavengeStateCommon {
		// Calculator iterates through the scavengable original streams and their metadata
		// it doesn't need to do anything with the metadata streams, accumulator has done those.
		IEnumerable<(StreamHandle<TStreamId>, OriginalStreamData)> OriginalStreamsToCalculate(
			StreamHandle<TStreamId> checkpoint);

		bool TryGetChunkTimeStampRange(int logicaChunkNumber, out ChunkTimeStampRange range);
	}

	public interface IScavengeStateForCalculator<TStreamId> :
		IScavengeStateForCalculatorReadOnly<TStreamId>,
		IIncreaseChunkWeights {

		void SetOriginalStreamDiscardPoints(
			StreamHandle<TStreamId> streamHandle,
			CalculationStatus status,
			DiscardPoint discardPoint,
			DiscardPoint maybeDiscardPoint);

		IEnumerable<TStreamId> LookupStreamIds(ulong streamHash);
	}

	public interface IIncreaseChunkWeights {
		void IncreaseChunkWeight(int logicalChunkNumber, float extraWeight);
	}

	public interface IScavengeStateForChunkExecutor<TStreamId> : IScavengeStateCommon {
		float SumChunkWeights(int startLogicalChunkNumber, int endLogicalChunkNumber);
		void ResetChunkWeights(int startLogicalChunkNumber, int endLogicalChunkNumber);
		bool TryGetChunkExecutionInfo(TStreamId streamId, out ChunkExecutionInfo info);
		bool TryGetMetastreamData(TStreamId streamId, out MetastreamData metastreamData);
	}

	public interface IScavengeStateForChunkMerger : IScavengeStateCommon {
	}

	public interface IScavengeStateForIndexExecutor<TStreamId> : IScavengeStateCommon {
		bool IsCollision(ulong streamHash);
		bool TryGetIndexExecutionInfo(
			StreamHandle<TStreamId> streamHandle,
			out IndexExecutionInfo info);
	}

	public interface IScavengeStateForCleaner : IScavengeStateCommon {
		bool AllChunksExecuted();
		void DeleteMetastreamData();
		void DeleteOriginalStreamData(bool deleteArchived);
	}
}
