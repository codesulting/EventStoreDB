﻿using EventStore.Core.TransactionLog.Scavenging;
using Xunit;

namespace EventStore.Core.XUnit.Tests.Scavenge {
	// generally the properties we need of the CollisionManager are tested at a higher
	// level. but a couple of fiddly bits are checked in here
	public class CollisionMapTests {
		[Fact]
		public void sanity() {
			var collisions = new InMemoryScavengeMap<string, Unit>();
			var sut = GenSut(collisions);

			// add a non-collision for ab-1
			sut["ab-1"] = "foo";

			// value can be retrieved
			Assert.True(sut.TryGetValue("ab-1", out var actual));
			Assert.Equal("foo", actual);

			// "a-1" collides with "a-2".
			collisions["ab-1"] = Unit.Instance;
			collisions["ab-2"] = Unit.Instance;
			sut.NotifyCollision("ab-1");

			// value for "a-1" can still be retrieved
			Assert.True(sut.TryGetValue("ab-1", out actual));
			Assert.Equal("foo", actual);

			// "a-2" not retrieveable since we never set it
			Assert.False(sut.TryGetValue("ab-2", out _));
		}

		private static CollisionMap<string, string> GenSut(IScavengeMap<string, Unit> collisions) {
			var sut = new CollisionMap<string, string>(
				new HumanReadableHasher(),
				x => collisions.TryGetValue(x, out _),
				new InMemoryScavengeMap<ulong, string>(),
				new InMemoryScavengeMap<string, string>());

			return sut;
		}
	}

	public class OriginalStreamCollisionMapTests {
		[Fact]
		public void can_enumerate() {
			//qq try running against sqlite version
			var collisions = new InMemoryScavengeMap<string, Unit>();
			var sut = GenSut(collisions);

			collisions["ac-1"] = Unit.Instance;
			collisions["ac-2"] = Unit.Instance;
			collisions["ac-3"] = Unit.Instance;

			// non collisions
			sut["ad-4"] = new OriginalStreamData { MaxCount = 4, Status = CalculationStatus.Active };
			sut["ae-5"] = new OriginalStreamData { MaxCount = 5, Status = CalculationStatus.Active };
			sut["af-6"] = new OriginalStreamData { MaxCount = 6, Status = CalculationStatus.Active };
			// collisions
			sut["ac-1"] = new OriginalStreamData { MaxCount = 1, Status = CalculationStatus.Active };
			sut["ac-2"] = new OriginalStreamData { MaxCount = 2, Status = CalculationStatus.Active };
			sut["ac-3"] = new OriginalStreamData { MaxCount = 3, Status = CalculationStatus.Active };

			// no checkpoint
			Assert.Collection(
				sut.EnumerateActive(checkpoint: default),
				x => Assert.Equal("(Id: ac-1, 1)", $"({x.Item1}, {x.Item2.MaxCount})"),
				x => Assert.Equal("(Id: ac-2, 2)", $"({x.Item1}, {x.Item2.MaxCount})"),
				x => Assert.Equal("(Id: ac-3, 3)", $"({x.Item1}, {x.Item2.MaxCount})"),
				x => Assert.Equal("(Hash: 100, 4)", $"({x.Item1}, {x.Item2.MaxCount})"),
				x => Assert.Equal("(Hash: 101, 5)", $"({x.Item1}, {x.Item2.MaxCount})"),
				x => Assert.Equal("(Hash: 102, 6)", $"({x.Item1}, {x.Item2.MaxCount})"));

			// id checkpoint
			Assert.Collection(
				sut.EnumerateActive(checkpoint: StreamHandle.ForStreamId("ac-2")),
				x => Assert.Equal("(Id: ac-3, 3)", $"({x.Item1}, {x.Item2.MaxCount})"),
				x => Assert.Equal("(Hash: 100, 4)", $"({x.Item1}, {x.Item2.MaxCount})"),
				x => Assert.Equal("(Hash: 101, 5)", $"({x.Item1}, {x.Item2.MaxCount})"),
				x => Assert.Equal("(Hash: 102, 6)", $"({x.Item1}, {x.Item2.MaxCount})"));

			// hash checkpoint
			Assert.Collection(
				sut.EnumerateActive(checkpoint: StreamHandle.ForHash<string>(101)),
				x => Assert.Equal("(Hash: 102, 6)", $"({x.Item1}, {x.Item2.MaxCount})"));

			// end checkpoint
			Assert.Empty(sut.EnumerateActive(checkpoint: StreamHandle.ForHash<string>(102)));
		}

		private static OriginalStreamCollisionMap<string> GenSut(IScavengeMap<string, Unit> collisions) {
			var sut = new OriginalStreamCollisionMap<string>(
				new HumanReadableHasher(),
				x => collisions.TryGetValue(x, out _),
				new InMemoryOriginalStreamScavengeMap<ulong>(),
				new InMemoryOriginalStreamScavengeMap<string>());

			return sut;
		}
	}
}
