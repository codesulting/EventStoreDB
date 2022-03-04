﻿using System;
using System.Collections.Generic;
using EventStore.Core.Index.Hashes;

namespace EventStore.Core.TransactionLog.Scavenging {

	//qq try to let this just be a datastructure rather than contain scavenge logic
	public class InMemoryMagicMap<TStreamId> :
		IMagicForAccumulator<TStreamId>,
		IMagicForCalculator<TStreamId>,
		IMagicForExecutor {

		private readonly CollisionDetector<TStreamId> _collisionDetector;

		// these are what would be persisted
		private readonly InMemoryCollisionResolver<TStreamId, StreamData> _metadatas;

		// these would just be in mem even in proper implementation
		private readonly ILongHasher<TStreamId> _hasher;
		private readonly IIndexReaderForAccumulator<TStreamId> _indexReaderForAccumulator;

		public InMemoryMagicMap(
			ILongHasher<TStreamId> hasher,
			IIndexReaderForAccumulator<TStreamId> indexReaderForAccumulator) {

			//qq inject this so that in log v3 we can have a trivial implementation
			//qq to save us having to look up the stream names repeatedly
			// irl this would be a lru cache.
			var cache = new Dictionary<ulong, TStreamId>();
			_collisionDetector = new CollisionDetector<TStreamId>(HashInUseBefore);
			_metadatas = new(hasher);
			_hasher = hasher;
			_indexReaderForAccumulator = indexReaderForAccumulator;

			bool HashInUseBefore(TStreamId recordStream, long recordPosition, out TStreamId candidateCollidee) {
				var hash = _hasher.Hash(recordStream);

				if (cache.TryGetValue(hash, out candidateCollidee))
					return true;

				//qq look in the index for any record with the current hash up to the limit
				// if any exists then grab the stream name for it
				if (_indexReaderForAccumulator.HashInUseBefore(hash, recordPosition, out candidateCollidee)) {
					cache[hash] = candidateCollidee;
					return true;
				}

				cache[hash] = recordStream;
				candidateCollidee = default;
				return false;
			}
		}




		//
		// FOR ACCUMULATOR
		//

		public void NotifyForCollisions(TStreamId streamId, long position) {
			// but consider that when we add this to the collision detector, that there might be a collision
			// and we might need to promote some of the handles.
			//qq want to make use of the _s?
			_ = _collisionDetector.DetectCollisions(streamId, position, out _);
		}

		// the accumulator needs to be able to set and get the metadatas. it always has the stream ids.
		public StreamData GetStreamData(TStreamId streamId) {
			if (!_metadatas.TryGetValue(streamId, out var streamData))
				streamData = StreamData.Empty;
			return streamData;
		}

		// this sets the stream metadata in such a way that it can be retrieved later
		public void SetStreamData(TStreamId streamId, StreamData streamData) {
			//qq _metadatas[streamId] = streamData;
		}







		//
		// FOR CALCULATOR
		//

		// the calculator needs to get the accumulated data for each scavengeable stream
		// it does not have and does not need to know the non colliding stream names.
		public IEnumerable<(StreamHandle<TStreamId>, StreamData)> StreamsWithMetadata {
			//qq consider making this a method?
			//qqqq not all of the scavengable streams have streamdata - the metadata streams dont
			// whose responsibility is that to worry about the fact that metadata streams have fixed
			// metadata themselves? its probably not the datastructure
			// but we dont want to iterate through all the scavengeable steams looking up the metadata
			// we want to iterate through all the metadatas and then iterate through all the 
			// metadata streams. but that is a detail of the scavenge state.

			get {
				return _metadatas.Enumerate();
			}
		}

		public void SetDiscardPoint(StreamHandle<TStreamId> stream, DiscardPoint dp) {
			throw new NotImplementedException();
//			_scavengeableStreams[stream] = dp;
		}






		//
		// FOR EXECUTOR
		//

		public DiscardPoint GetDiscardPoint(StreamName streamName) {
			throw new NotImplementedException();
		}

		public DiscardPoint GetDiscardPoint(StreamHash streamHash) {
			throw new NotImplementedException();
		}

		public bool IsCollision(StreamHash streamHash) {
			throw new NotImplementedException();
		}

		public void Add(StreamName streamName) {
			throw new NotImplementedException();
		}
	}
}
