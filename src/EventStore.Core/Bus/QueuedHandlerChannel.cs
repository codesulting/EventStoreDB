using System;
using System.Threading;
using EventStore.Common.Utils;
using EventStore.Core.Messages;
using EventStore.Core.Messaging;
using EventStore.Core.Services.Monitoring.Stats;
using System.Threading.Tasks;
using ILogger = Serilog.ILogger;
using System.Threading.Channels;

namespace EventStore.Core.Bus {
	/// <summary>
	/// Channels based queue which passes messages
	/// to the consumer. It also tracks statistics about the message processing to help
	/// in identifying bottlenecks
	/// </summary>
	public class QueuedHandlerChannel : IQueuedHandler, IHandle<Message>, IPublisher, IMonitoredQueue,
		IThreadSafePublisher {
		private static readonly ILogger Log = Serilog.Log.ForContext<QueuedHandlerMRES>();
		private Channel<Message> _channel;
		public int MessageCount {
			get { return _channel.Reader.Count; }
		}

		public string Name {
			get { return _queueStats.Name; }
		}

		private readonly IHandle<Message> _consumer;

		private readonly bool _watchSlowMsg;
		private readonly TimeSpan _slowMsgThreshold;
		private readonly bool _bounded;
		private readonly ChannelOptions _config;
		private volatile bool _stop;
		private readonly ManualResetEventSlim _stopped = new ManualResetEventSlim(true);
		private int? _maxCapacity = null;

		private readonly QueueMonitor _queueMonitor;
		private readonly QueueStatsCollector _queueStats;
		private readonly TaskCompletionSource<object> _tcs = new TaskCompletionSource<object>();
		private CancellationTokenSource _tokenSource;

		//todo-clc: break this into Bounded and Unbounded Queue classes
		//also this is currently mapping into existing patterns for queue monitoring that should be reviewed and updated making this far simpler
		public QueuedHandlerChannel(
			IHandle<Message> consumer,
			string name,
			QueueStatsManager queueStatsManager,
			bool watchSlowMsg = true,
			TimeSpan? slowMsgThreshold = null,
			string groupName = null,
			bool bounded = false,
			ChannelOptions config = null) {
			Ensure.NotNull(consumer, "consumer");
			Ensure.NotNull(name, "name");

			_consumer = consumer;
			if (bounded) {
				if (config as BoundedChannelOptions == null) { throw new ArgumentNullException(nameof(config), "Bounded channels require explict BoundedChannelOption config"); }

			}
			if (!bounded && config == null) {
				config = new UnboundedChannelOptions();
			}
			_config = config;
			_watchSlowMsg = watchSlowMsg;
			_slowMsgThreshold = slowMsgThreshold ?? InMemoryBus.DefaultSlowMessageThreshold;
			_bounded = bounded;
			_queueMonitor = QueueMonitor.Default;
			_queueStats = queueStatsManager.CreateQueueStatsCollector(name, groupName);
			_channel = BuildChannel();
		}

		private Channel<Message> BuildChannel() {
			if (_bounded) {
				return Channel.CreateBounded<Message>((BoundedChannelOptions)_config);				
			} else {
				return Channel.CreateUnbounded<Message>((UnboundedChannelOptions)_config);
			}
		}

		public Task Start() {
			if (!_stopped.IsSet)
				throw new InvalidOperationException("Channel already running.");

			_stop = false;
			_stopped.Reset();
			_queueMonitor.Register(this);			
			_tokenSource = new CancellationTokenSource();
			Task.Run(() => ReadFromQueue(_tokenSource.Token));
			return _tcs.Task;
		}

		public void Stop() {
			_stop = true;
			_channel.Writer.TryComplete();
		}

		public void RequestStop() {
			_stop = true;
			_channel.Writer.TryComplete();
		}

		private async void ReadFromQueue(CancellationToken cancelToken) {
			try {
				_queueStats.Start();

				while (!_stop) {
					Message msg = null;
					try {
						_queueStats.EnterIdle();
						//We have a dedicated thread, it's faster to keep using it, and that's kind of the point
#pragma warning disable CAC001 // ConfigureAwaitChecker
						msg = await _channel.Reader.ReadAsync(cancelToken);
#pragma warning restore CAC001 // ConfigureAwaitChecker		
						if (cancelToken.IsCancellationRequested) { throw new TaskCanceledException(); }
						_queueStats.EnterBusy();
#if DEBUG
							_queueStats.Dequeued(msg);
#endif
						var queueDepth = _channel.Reader.Count;
						_queueStats.ProcessingStarted(msg.GetType(), queueDepth);

						if (_watchSlowMsg) {
							var start = DateTime.UtcNow;

							_consumer.Handle(msg);

							var elapsed = DateTime.UtcNow - start;
							if (elapsed > _slowMsgThreshold) {
								var curQueueDepth = _channel.Reader.Count;
								var maxCapacity = _maxCapacity.HasValue ? _maxCapacity.Value.ToString() : "unbounded";
								Log.Debug(
									"SLOW QUEUE MSG [{queue}]: {message} - {elapsed}ms. Q: capacity{maxCapacity} {prevQueueCount}/{curQueueCount}.",
									Name, _queueStats.InProgressMessage.Name, (int)elapsed.TotalMilliseconds, maxCapacity, queueDepth,
									curQueueDepth);
								if (elapsed > QueuedHandler.VerySlowMsgThreshold &&
									!(msg is SystemMessage.SystemInit))
									Log.Error(
										"---!!! VERY SLOW QUEUE MSG [{queue}]: {message} - {elapsed}ms. Q: capacity{maxCapacity} {prevQueueCount}/{curQueueCount}.",
										Name, _queueStats.InProgressMessage.Name, (int)elapsed.TotalMilliseconds, maxCapacity,
										queueDepth, curQueueDepth);
							}
						} else {
							_consumer.Handle(msg);
						}

						_queueStats.ProcessingEnded(1);
					} catch (TaskCanceledException) {
						throw;
					} catch (Exception ex) {
						Log.Error(ex, "Error while processing message {message} in queued handler '{queue}'.",
							msg, Name);
#if DEBUG
						throw;
#endif
					}
				}
			} catch (TaskCanceledException) {
				_channel.Writer.TryComplete();
				//drop old channel and create a new one
				_channel = BuildChannel();
				//just exit
			} catch (Exception ex) {
				_tcs.TrySetException(ex);
				throw;
			} finally {
				_stopped.Set();
				_queueStats.Stop();
				_queueMonitor.Unregister(this);

			}
		}

		public void Publish(Message message) {
			//Ensure.NotNull(message, "message");
#if DEBUG
			_queueStats.Enqueued();
#endif

			//sync & blocking to match current behavior and there is no context here		
			_channel.Writer.WriteAsync(message).GetAwaiter().GetResult();

		}

		public void Handle(Message message) {
			Publish(message);
		}

		public QueueStats GetStatistics() {
			return _queueStats.GetStatistics(_channel.Reader.Count);
		}
	}
}
