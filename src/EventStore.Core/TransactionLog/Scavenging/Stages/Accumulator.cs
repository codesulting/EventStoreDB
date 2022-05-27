using System;
using System.Diagnostics;
using System.Threading;
using EventStore.Common.Log;
using EventStore.Core.Helpers;
using EventStore.Core.LogAbstraction;

namespace EventStore.Core.TransactionLog.Scavenging {
	public class Accumulator {
		protected static readonly ILogger Log = LogManager.GetLoggerFor<Accumulator>();
	}

	public class Accumulator<TStreamId> : Accumulator, IAccumulator<TStreamId> {

		private readonly int _chunkSize;
		private readonly IMetastreamLookup<TStreamId> _metastreamLookup;
		private readonly IChunkReaderForAccumulator<TStreamId> _chunkReader;
		private readonly IIndexReaderForAccumulator<TStreamId> _index;
		private readonly int _cancellationCheckPeriod;

		public Accumulator(
			int chunkSize,
			IMetastreamLookup<TStreamId> metastreamLookup,
			IChunkReaderForAccumulator<TStreamId> chunkReader,
			IIndexReaderForAccumulator<TStreamId> index,
			int cancellationCheckPeriod) {

			_chunkSize = chunkSize;
			_metastreamLookup = metastreamLookup;
			_chunkReader = chunkReader;
			_index = index;
			_cancellationCheckPeriod = cancellationCheckPeriod;
		}

		// Start a new accumulation
		public void Accumulate(
			ScavengePoint prevScavengePoint,
			ScavengePoint scavengePoint,
			IScavengeStateForAccumulator<TStreamId> state,
			CancellationToken cancellationToken) {

			Log.Trace("Starting new scavenge accumulation phase: {prevScavengePoint} to {scavengePoint}",
				prevScavengePoint?.GetName() ?? "beginning of log",
				scavengePoint.GetName());

			var doneLogicalChunkNumber = default(int?);

			// accumulate the chunk that the previous scavenge point was in because we last time we
			// only accumulated that chunk _up to_ the scavenge point and not all of it.
			if (prevScavengePoint != null) {
				if (prevScavengePoint.Position >= _chunkSize) {
					doneLogicalChunkNumber = (int)(prevScavengePoint.Position / _chunkSize) - 1;
				} else {
					// the previous scavenge point was in the first chunk so we haven't
					// 'done' any chunks, leave it as null.
				}
			}

			var checkpoint = new ScavengeCheckpoint.Accumulating(
					scavengePoint,
					doneLogicalChunkNumber);
			state.SetCheckpoint(checkpoint);
			Accumulate(checkpoint, state, cancellationToken);
		}

		// Continue accumulation for a particular scavenge point
		public void Accumulate(
			ScavengeCheckpoint.Accumulating checkpoint,
			IScavengeStateForAccumulator<TStreamId> state,
			CancellationToken cancellationToken) {

			Log.Trace("Accumulating from checkpoint: {checkpoint}", checkpoint);
			var stopwatch = new Stopwatch();

			// bounds are ok because we wont try to read past the scavenge point
			var logicalChunkNumber = checkpoint.DoneLogicalChunkNumber + 1 ?? 0;
			var scavengePoint = checkpoint.ScavengePoint;
			var weights = new WeightAccumulator(state);

			// reusable objects to avoid GC pressure
			var originalStreamRecord = ReusableObject.Create(
				new RecordForAccumulator<TStreamId>.OriginalStreamRecord());
			var metadataStreamRecord = ReusableObject.Create(
				new RecordForAccumulator<TStreamId>.MetadataStreamRecord());
			var tombstoneRecord = ReusableObject.Create(
				new RecordForAccumulator<TStreamId>.TombStoneRecord());

			while (AccumulateChunkAndRecordRange(
					scavengePoint,
					state,
					weights,
					logicalChunkNumber,
					originalStreamRecord,
					metadataStreamRecord,
					tombstoneRecord,
					stopwatch,
					cancellationToken)) {
				logicalChunkNumber++;
			}
		}

		private bool AccumulateChunkAndRecordRange(
			ScavengePoint scavengePoint,
			IScavengeStateForAccumulator<TStreamId> state,
			WeightAccumulator weights,
			int logicalChunkNumber,
			ReusableObject<RecordForAccumulator<TStreamId>.OriginalStreamRecord> originalStreamRecord,
			ReusableObject<RecordForAccumulator<TStreamId>.MetadataStreamRecord> metadataStreamRecord,
			ReusableObject<RecordForAccumulator<TStreamId>.TombStoneRecord> tombStoneRecord,
			Stopwatch stopwatch,
			CancellationToken cancellationToken) {

			stopwatch.Restart();

			// for correctness it is important that any particular DetectCollisions call is contained
			// within a transaction.
			var transaction = state.BeginTransaction();
			try {
				var ret = AccumulateChunk(
					scavengePoint,
					state,
					weights,
					logicalChunkNumber,
					originalStreamRecord,
					metadataStreamRecord,
					tombStoneRecord,
					cancellationToken,
					out var chunkMinTimeStamp,
					out var chunkMaxTimeStamp);

				if (chunkMinTimeStamp <= chunkMaxTimeStamp) {
					state.SetChunkTimeStampRange(
						logicalChunkNumber: logicalChunkNumber,
						new ChunkTimeStampRange(
							min: chunkMinTimeStamp,
							max: chunkMaxTimeStamp));
				} else {
					// empty range, no need to store it.
				}

				var accumulationElapsed = stopwatch.Elapsed;

				weights.Flush();
				transaction.Commit(new ScavengeCheckpoint.Accumulating(
					scavengePoint,
					doneLogicalChunkNumber: logicalChunkNumber));

				Log.Trace("Accumulated chunk {chunk} in {elapsed}. Chunk total: {chunkTotalElapsed}",
					logicalChunkNumber, accumulationElapsed, stopwatch.Elapsed);

				return ret;
			} catch {
				transaction.Rollback();
				throw;
			}
		}

		// returns true to continue
		// do not assume that record TimeStamps are non descending, clocks can change.
		private bool AccumulateChunk(
			ScavengePoint scavengePoint,
			IScavengeStateForAccumulator<TStreamId> state,
			WeightAccumulator weights,
			int logicalChunkNumber,
			ReusableObject<RecordForAccumulator<TStreamId>.OriginalStreamRecord> originalStreamRecord,
			ReusableObject<RecordForAccumulator<TStreamId>.MetadataStreamRecord> metadataStreamRecord,
			ReusableObject<RecordForAccumulator<TStreamId>.TombStoneRecord> tombStoneRecord,
			CancellationToken cancellationToken,
			out DateTime chunkMinTimeStamp,
			out DateTime chunkMaxTimeStamp) {

			// start with empty range and expand it as we discover records.
			chunkMinTimeStamp = DateTime.MaxValue;
			chunkMaxTimeStamp = DateTime.MinValue;

			var stopBefore = scavengePoint.Position;
			var cancellationCheckCounter = 0;
			foreach (var record in _chunkReader.ReadChunk(
				         logicalChunkNumber,
				         originalStreamRecord,
				         metadataStreamRecord,
				         tombStoneRecord)) {
				using (record) {
					if (record.LogPosition >= stopBefore)
						return false;

					if (record.TimeStamp < chunkMinTimeStamp)
						chunkMinTimeStamp = record.TimeStamp;

					if (record.TimeStamp > chunkMaxTimeStamp)
						chunkMaxTimeStamp = record.TimeStamp;

					switch (record) {
						case RecordForAccumulator<TStreamId>.OriginalStreamRecord x:
							ProcessOriginalStreamRecord(x, state);
							originalStreamRecord.Release();
							break;
						case RecordForAccumulator<TStreamId>.MetadataStreamRecord x:
							ProcessMetastreamRecord(x, scavengePoint, state, weights);
							metadataStreamRecord.Release();
							break;
						case RecordForAccumulator<TStreamId>.TombStoneRecord x:
							ProcessTombstone(x, scavengePoint, state, weights);
							tombStoneRecord.Release();
							break;
						default:
							throw new InvalidOperationException($"Unexpected record: {record}");
					}
				}

				if (++cancellationCheckCounter == _cancellationCheckPeriod) {
					cancellationCheckCounter = 0;
					cancellationToken.ThrowIfCancellationRequested();
				}
			}

			return true;
		}

		// For every* record in an original stream we need to see if its stream collides.
		// its not so bad, because we have a cache
		// * maybe not every.. beyond a certain point we only need to check records with eventnumber 0
		//    (and perhaps -1 to cover transactions)
		//    but doing so has some complications
		//      - have to identify the point at which we can switch to just checking 0s
		//qq    - is it possible that a new stream starts at non-zero without already having been checked
		//        before? seems unlikely.. 
		private static void ProcessOriginalStreamRecord(
			RecordForAccumulator<TStreamId>.OriginalStreamRecord record,
			IScavengeStateForAccumulator<TStreamId> state) {

			state.DetectCollisions(record.StreamId);
		}

		// For every record in a metadata stream
		//   - check if the metastream or originalstream collide with anything
		//   - store the metadata against the original stream so the calculator can calculate the
		//         discard point.
		//   - update the discard point of the metadatastream
		//   - increase the weight of the chunk with the old metadata if applicable
		// the actual type of the record isn't relevant. if it is in a metadata stream it affects
		// the metadata. if its data parses to streammetadata then thats the metadata. if it doesn't
		// parse, then it clears the metadata.
		private void ProcessMetastreamRecord(
			RecordForAccumulator<TStreamId>.MetadataStreamRecord record,
			ScavengePoint scavengePoint,
			IScavengeStateForAccumulator<TStreamId> state,
			WeightAccumulator weights) {

			var originalStreamId = _metastreamLookup.OriginalStreamOf(record.StreamId);
			state.DetectCollisions(originalStreamId);
			state.DetectCollisions(record.StreamId);

			if (record.EventNumber < 0)
				throw new InvalidOperationException(
					$"Found metadata in transaction in stream {record.StreamId}");

			CheckMetadataOrdering(
				record,
				state.GetStreamHandle(record.StreamId),
				scavengePoint,
				out var isInOrder,
				out var replacedPosition);

			if (replacedPosition.HasValue) {
				var logicalChunkNumber = (int)(replacedPosition.Value / _chunkSize);
				weights.OnDiscard(logicalChunkNumber: logicalChunkNumber);
			}

			if (!isInOrder) {
				Log.Warn("Accumulator found out of order metadata: {stream}:{eventNumber}",
					record.StreamId,
					record.EventNumber);
				return;
			}

			if (_metastreamLookup.IsMetaStream(originalStreamId)) {
				// record in a metadata stream of a metadata stream: $$$$xyz
				// this does not set metadata for $$xyz (which is fixed at maxcount1)
				// (see IndexReader.GetStreamMetadataCached)
				// but it does, itself, have a fixed metadta of maxcount1, so move the discard point.
			} else {
				// record is in a standard metadata stream: $$xyz
				// Update the Metadata for stream xyz
				state.SetOriginalStreamMetadata(originalStreamId, record.Metadata);
			}

			// Update the discard point
			var discardPoint = DiscardPoint.DiscardBefore(record.EventNumber);
			state.SetMetastreamDiscardPoint(record.StreamId, discardPoint);
		}

		// For every tombstone
		//   - check if the stream collides
		//   - set the istombstoned flag to true
		//   - increase the weight of the chunk with the old metadata if applicable
		private void ProcessTombstone(
			RecordForAccumulator<TStreamId>.TombStoneRecord record,
			ScavengePoint scavengePoint,
			IScavengeStateForAccumulator<TStreamId> state,
			WeightAccumulator weights) {

			state.DetectCollisions(record.StreamId);

			if (_metastreamLookup.IsMetaStream(record.StreamId)) {
				// isn't possible to write a tombstone to a metadatastream, but spot it in case
				// it ever was possible.
				throw new InvalidOperationException(
					$"Found Tombstone in metadata stream {record.StreamId}");
			}

			if (record.EventNumber < 0) {
				throw new InvalidOperationException(
					$"Found Tombstone in transaction in stream {record.StreamId}");
			}

			var originalStreamId = record.StreamId;
			state.SetOriginalStreamTombstone(originalStreamId);

			var metastreamId = _metastreamLookup.MetaStreamOf(originalStreamId);
			state.SetMetastreamTombstone(metastreamId);
			state.DetectCollisions(metastreamId); // required before we grab a stream handle below

			// unlike metadata, a tombstone still takes effect even it is out of order in the log
			// (because the index will still bless it with a max event number in the indexentry)
			// so we don't need to check for order, but just need to get the last metadata record
			// if any, and add weight for it.
			// note that the metadata record is in a different stream to the tombstone
			// note that since it is tombstoned, there wont be more metadata records coming so
			// the last one really is the one we want.
			var eventInfos = _index.ReadEventInfoBackward(
				streamId: metastreamId,
				handle: state.GetStreamHandle(metastreamId),
				fromEventNumber: -1, // last
				maxCount: 1,
				scavengePoint: scavengePoint);

			foreach (var eventInfo in eventInfos) {
				var logicalChunkNumber = (int)(eventInfo.LogPosition / _chunkSize);
				weights.OnDiscard(logicalChunkNumber: logicalChunkNumber);
			}
		}

		private void CheckMetadataOrdering(
			RecordForAccumulator<TStreamId>.MetadataStreamRecord record,
			StreamHandle<TStreamId> metastreamId,
			ScavengePoint scavengePoint,
			out bool isInOrder,
			out long? replacedPosition) {

			// We have just received a metadata record.
			// we need to achieve two things here
			// 1. determine if this record is in order. if not we will skip over it and not apply it.
			// 2. add appropriate weights
			//     - this is the first metadata record -> no weight to add.
			//     - we are displacing a record -> add weight to its chunk
			//     - we are skipping over this record -> add weight to this records chunk
			//
			// todo: consider searching the rest of the eventInfos in the stream, not just 100 events
			// but the chances of that many consecutive invalid metastream records being written is slim,
			// and if the writes didn't produce the desired effect it is likely the user wrote the
			// metadata successfully afterwards anyway.

			// start from the event before us if possible, to see which event we are replacing.
			var fromEventNumber = record.EventNumber == 0
				? record.EventNumber
				: record.EventNumber - 1;

			var eventInfos = _index.ReadEventInfoForward(
				handle: metastreamId,
				fromEventNumber: fromEventNumber,
				maxCount: 100,
				scavengePoint: scavengePoint);

			isInOrder = true;
			foreach (var eventInfo in eventInfos) {
				if (eventInfo.LogPosition < record.LogPosition &&
					eventInfo.EventNumber >= record.EventNumber) {

					// found an event that is before us in the log but has our event number or higher.
					// that record is the metadata that we will keep, skipping over this one.
					isInOrder = false;
				}
			}

			if (isInOrder) {
				if (eventInfos.Length > 0 &&
					eventInfos[0].EventNumber < record.EventNumber &&
					eventInfos[0].LogPosition < record.LogPosition) {
					replacedPosition = eventInfos[0].LogPosition;
				} else {
					replacedPosition = null;
				}
			} else {
				replacedPosition = record.LogPosition;
			}
		}
	}
}
