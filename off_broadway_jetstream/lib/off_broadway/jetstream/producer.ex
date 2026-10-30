defmodule OffBroadway.Jetstream.Producer do
  @moduledoc """
  A Broadway producer that pulls messages from a NATS JetStream durable pull consumer.

  Every producer of the pipeline subscribes to its own inbox and long-polls the same
  durable consumer with pull requests of up to `:max_batch_size` messages, bounded by the
  demand of the pipeline. JetStream spreads the messages of the consumer across all
  producers, so the pipeline can run any number of producers (`concurrency`), and several
  nodes can run the same pipeline against the same consumer.

  The producer talks to NATS through a `Gnat` connection that you start and supervise
  yourself, usually with `Gnat.ConnectionSupervisor`, before the pipeline:

      children = [
        {Gnat.ConnectionSupervisor,
         %{name: :gnat, connection_settings: [%{host: "localhost", port: 4222}]}},
        MyBroadway
      ]

  When the connection goes down, the producer waits for `Gnat.ConnectionSupervisor` to
  bring it back and resumes pulling. Messages that were in flight are redelivered by
  JetStream after the consumer's `ack_wait`.

  ## Options

  #{NimbleOptions.docs(OffBroadway.Jetstream.Options.definition())}

  ## Consumer requirements

  The consumer must be a durable pull consumer (no `deliver_subject`) with
  `ack_policy: :explicit`. All producers of the pipeline share it. Either create it
  upfront, for example with the `nats` CLI:

      nats consumer add ORDERS order-worker --pull --ack explicit --max-deliver 5

  or pass a `Gnat.Jetstream.API.Consumer` struct as the `:consumer` option and let the
  producers create it.

  JetStream stops delivering once a consumer has `max_ack_pending` unacknowledged
  messages, so set it to at least the number of messages the pipeline holds in flight
  (all processors and batchers). If the consumer sets `max_batch`, keep `:max_batch_size`
  less than or equal to it, otherwise the server rejects the pull requests with
  `409 Exceeded MaxRequestBatch`.

  ### Redelivery

  A message must be acknowledged within the consumer's `ack_wait` (30 seconds by
  default), counted from its delivery. The producer does not extend that window, so a
  message still sitting in a processor or batcher when `ack_wait` elapses is delivered
  again, possibly to another producer, while the first copy is still being handled. Set
  `ack_wait` above your longest handling time plus `batch_timeout`, and make handlers
  idempotent, for example keyed on `message.metadata.jetstream.stream_seq` or on a
  `Nats-Msg-Id` header set by the publisher.

  `max_deliver` is unlimited (`-1`) unless the consumer sets it, both in NATS and in
  `Gnat.Jetstream.API.Consumer`. With the default `on_failure: :nack`, a message that
  always fails is then redelivered immediately and forever. Always set `max_deliver`,
  and use `{:nack, delay_ms}` to space out retries: a plain `:nack` ignores the consumer's
  `backoff`, which only applies to messages left unacknowledged past `ack_wait` (`:noop`).

  JetStream has no dead-letter queue. When a message reaches `max_deliver`, the server
  publishes an advisory on `$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.<stream>.<consumer>`
  whose `stream_seq` identifies the message. Subscribe to it and fetch the message with
  `Gnat.Jetstream.API.Stream.get_message/4` to handle dead letters.

  ## Acknowledgements

  You can use the `:on_success` and `:on_failure` options to control how messages are
  acknowledged. You can set these options when starting the producer or change them for
  each message through `Broadway.Message.configure_ack/2`. By default, successful
  messages are acked (`:ack`) and failed messages are negatively acknowledged (`:nack`).

  The possible values for `:on_success` and `:on_failure` are:

    * `:ack` - acknowledges the message. JetStream will not redeliver it.

    * `:nack` - negatively acknowledges the message. JetStream redelivers it right away,
      up to the consumer's `max_deliver` (unlimited by default, see "Redelivery").

    * `{:nack, delay_ms}` - negatively acknowledges the message. JetStream redelivers
      it after `delay_ms` milliseconds, up to the consumer's `max_deliver`.

    * `:term` - terminates the message. JetStream will not redeliver it, but it is not
      considered successfully processed either (it is reported as an advisory).

    * `:noop` - does nothing. JetStream redelivers the message once the consumer's
      `ack_wait` elapses.

  For example, to give a failed message a minute before it is retried:

      def handle_message(_, message, _) do
        case process(message.data) do
          :ok ->
            message

          {:error, :rate_limited} ->
            message
            |> Broadway.Message.configure_ack(on_failure: {:nack, 60_000})
            |> Broadway.Message.failed(:rate_limited)
        end
      end

  ## Message metadata

  Each `Broadway.Message` has the following `metadata`:

    * `:subject` - the subject the message was published to.
    * `:headers` - the message headers as a list of `{name, value}` tuples (`[]` when
      there are none).
    * `:reply_to` - the `$JS.ACK.…` subject used to acknowledge the message.
    * `:jetstream` - a `Gnat.Jetstream.API.Message.Metadata` struct with the stream and
      consumer sequences, the number of deliveries, the number of pending messages and the
      timestamp of the message.

  ## Example

      defmodule MyBroadway do
        use Broadway

        def start_link(_opts) do
          Broadway.start_link(__MODULE__,
            name: __MODULE__,
            producer: [
              module:
                {OffBroadway.Jetstream.Producer,
                 connection_name: :gnat,
                 stream_name: "ORDERS",
                 consumer_name: "order-worker"},
              concurrency: 2
            ],
            processors: [
              default: [concurrency: 10]
            ]
          )
        end

        @impl true
        def handle_message(_processor, message, _context) do
          IO.inspect(message.data, label: "Got message")
          message
        end
      end
  """

  use GenStage

  require Logger

  alias Broadway.Message
  alias Gnat.Jetstream.API.Consumer
  alias NimbleOptions.ValidationError
  alias OffBroadway.Jetstream.{Acknowledger, Options}

  @behaviour Broadway.Producer

  # NATS JetStream error codes meaning the consumer already exists with another config.
  @consumer_exists_err_codes [10013, 10105, 10148]

  @impl Broadway.Producer
  def prepare_for_start(_module, broadway_opts) do
    {__MODULE__, opts} = broadway_opts[:producer][:module]

    opts =
      try do
        Options.validate!(opts)
      rescue
        error in ValidationError -> raise ArgumentError, format_error(error)
      end

    Acknowledger.put_config(ack_ref(broadway_opts[:name]), %{
      connection_name: opts[:connection_name],
      on_success: opts[:on_success],
      on_failure: opts[:on_failure]
    })

    {[], put_in(broadway_opts, [:producer, :module], {__MODULE__, opts})}
  end

  defp format_error(%ValidationError{keys_path: [], message: message}) do
    "invalid configuration given to OffBroadway.Jetstream.Producer.prepare_for_start/2, " <>
      message
  end

  defp format_error(%ValidationError{keys_path: keys_path, message: message}) do
    "invalid configuration given to OffBroadway.Jetstream.Producer.prepare_for_start/2 " <>
      "for key #{inspect(keys_path)}, " <> message
  end

  defp ack_ref(broadway_name), do: {Acknowledger, broadway_name}

  @impl GenStage
  def init(opts) do
    {broadway, opts} = Keyword.pop!(opts, :broadway)
    send(self(), :connect)

    {:producer,
     %{
       opts: opts,
       ack_ref: ack_ref(broadway[:name]),
       inbox: opts[:inbox_prefix] <> Base.url_encode64(:crypto.strong_rand_bytes(12)),
       status: :disconnected,
       conn_pid: nil,
       conn_ref: nil,
       sid: nil,
       retries_left: opts[:connection_retries],
       demand: 0,
       pull: nil,
       timer: nil,
       draining: false
     }}
  end

  @impl GenStage
  def handle_demand(incoming_demand, %{demand: demand} = state) do
    {:noreply, [], maybe_pull(%{state | demand: demand + incoming_demand})}
  end

  @impl GenStage
  def handle_info(:connect, %{draining: true} = state), do: {:noreply, [], state}
  def handle_info(:connect, %{status: :connected} = state), do: {:noreply, [], state}
  def handle_info(:connect, state), do: connect(state)

  def handle_info(:pull, state), do: {:noreply, [], maybe_pull(%{state | timer: nil})}

  def handle_info({:msg, %{sid: sid, status: status} = msg}, %{sid: sid} = state)
      when is_binary(status) and status != "" do
    handle_status(status, Map.get(msg, :description), state)
  end

  def handle_info({:msg, %{sid: sid} = msg}, %{sid: sid, draining: true} = state) do
    # A message from the last pull request that was already in our mailbox when draining
    # started. Hand it back so another consumer gets it right away.
    nack(state.conn_pid, msg.reply_to)
    {:noreply, [], state}
  end

  def handle_info({:msg, %{sid: sid} = msg}, %{sid: sid} = state) do
    message = %Message{
      data: msg.body,
      metadata: %{
        subject: msg.topic,
        headers: Map.get(msg, :headers, []),
        reply_to: msg.reply_to,
        jetstream: jetstream_metadata(msg)
      },
      acknowledger: {Acknowledger, state.ack_ref, %{reply_to: msg.reply_to}}
    }

    state =
      case state.pull do
        %{remaining: 1} -> %{state | pull: nil}
        %{remaining: remaining} -> %{state | pull: %{remaining: remaining - 1}}
        nil -> state
      end

    {:noreply, [message], maybe_pull(%{state | demand: max(state.demand - 1, 0)})}
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, %{conn_ref: ref} = state) do
    Logger.debug(
      "OffBroadway.Jetstream.Producer lost its Gnat connection (#{inspect(reason)}); reconnecting"
    )

    cancel_timer(state.timer)
    Process.send_after(self(), :connect, state.opts[:connection_retry_timeout])

    {:noreply, [],
     %{disconnected(state) | timer: nil, retries_left: state.opts[:connection_retries]}}
  end

  # Messages for a subscription we already left and monitors of previous connections.
  def handle_info(_msg, state), do: {:noreply, [], state}

  @impl Broadway.Producer
  def prepare_for_draining(state) do
    cancel_timer(state.timer)

    # Leaving the inbox makes the server drop our outstanding pull request. The sid is kept
    # so messages that are already on their way are recognized and handed back.
    if state.status == :connected do
      unsub(state.conn_pid, state.sid)
    end

    {:noreply, [], %{state | draining: true, timer: nil, pull: nil}}
  end

  ## Connecting

  defp connect(%{opts: opts} = state) do
    with {:ok, conn} <- whereis(opts[:connection_name]),
         :ok <- ensure_consumer(conn, opts),
         {:ok, sid} <- subscribe(conn, state.inbox) do
      state = %{
        state
        | status: :connected,
          conn_pid: conn,
          conn_ref: Process.monitor(conn),
          sid: sid,
          retries_left: opts[:connection_retries],
          pull: nil
      }

      {:noreply, [], maybe_pull(state)}
    else
      {:error, :no_connection} ->
        retry_connect(state, {:shutdown, :connection_failed}, fn ->
          "OffBroadway.Jetstream.Producer could not reach Gnat connection " <>
            "#{inspect(opts[:connection_name])} after #{opts[:connection_retries]} retries"
        end)

      {:error, reason} ->
        Logger.error(
          "OffBroadway.Jetstream.Producer could not use consumer #{opts[:consumer_name]} " <>
            "on stream #{opts[:stream_name]}: #{inspect(reason)}"
        )

        retry_connect(state, {:shutdown, {:consumer_unavailable, reason}}, nil)

      {:stop, reason} ->
        {:stop, {:shutdown, reason}, state}
    end
  end

  defp retry_connect(%{retries_left: 0} = state, stop_reason, error_message) do
    if error_message, do: Logger.error(error_message.())
    {:stop, stop_reason, state}
  end

  defp retry_connect(%{retries_left: retries_left, opts: opts} = state, _stop_reason, _message) do
    Logger.debug(
      "OffBroadway.Jetstream.Producer retrying to connect in #{opts[:connection_retry_timeout]}ms " <>
        "(#{retries_left} retries left)"
    )

    Process.send_after(self(), :connect, opts[:connection_retry_timeout])
    {:noreply, [], %{state | retries_left: retries_left - 1}}
  end

  defp whereis(name) do
    case GenServer.whereis(name) do
      pid when is_pid(pid) -> {:ok, pid}
      _ -> {:error, :no_connection}
    end
  end

  defp ensure_consumer(conn, opts) do
    case opts[:consumer] do
      %Consumer{} = consumer -> create_consumer(conn, consumer)
      nil -> check_consumer(conn, opts)
    end
  catch
    # The connection went away between the lookup and the request.
    :exit, _reason -> {:error, :no_connection}
  end

  defp create_consumer(conn, consumer) do
    case Consumer.create(conn, consumer) do
      {:ok, _info} ->
        :ok

      {:error, %{"err_code" => code}} when code in @consumer_exists_err_codes ->
        Logger.warning(
          "OffBroadway.Jetstream.Producer consumer #{consumer.durable_name} already exists " <>
            "on stream #{consumer.stream_name} with a different configuration; " <>
            "using the existing consumer"
        )

        :ok

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp check_consumer(conn, opts) do
    case Consumer.info(conn, opts[:stream_name], opts[:consumer_name], opts[:domain]) do
      {:ok, %{config: %{ack_policy: :explicit}}} ->
        :ok

      {:ok, %{config: %{ack_policy: policy}}} ->
        Logger.error(
          "OffBroadway.Jetstream.Producer consumer #{opts[:consumer_name]} on stream " <>
            "#{opts[:stream_name]} has ack_policy #{inspect(policy)}; :explicit is required"
        )

        {:stop, {:unsupported_ack_policy, policy}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp subscribe(conn, inbox) do
    Gnat.sub(conn, self(), inbox)
  catch
    :exit, _reason -> {:error, :no_connection}
  end

  ## Pulling

  defp maybe_pull(
         %{status: :connected, draining: false, pull: nil, timer: nil, demand: demand} = state
       )
       when demand > 0 do
    %{opts: opts} = state
    batch = min(demand, opts[:max_batch_size])

    request_opts = [batch: batch, expires: opts[:request_expires] * 1_000_000]

    try do
      Consumer.request_next_message(
        state.conn_pid,
        opts[:stream_name],
        opts[:consumer_name],
        state.inbox,
        opts[:domain],
        request_opts
      )
    catch
      # The connection is gone; its :DOWN message triggers the reconnect.
      :exit, _reason -> :ok
    end

    %{state | pull: %{remaining: batch}}
  end

  defp maybe_pull(state), do: state

  # 404 (no messages, only for no_wait requests) and 408 (request expired) end the pull
  # request: issue the next one right away, which turns pulling into a long-poll loop.
  defp handle_status(status, _description, state) when status in ["404", "408"] do
    {:noreply, [], maybe_pull(%{state | pull: nil})}
  end

  # Idle heartbeat. Not requested by this producer.
  defp handle_status("100", _description, state), do: {:noreply, [], state}

  defp handle_status("409", "Consumer Deleted", state) do
    Logger.warning(
      "OffBroadway.Jetstream.Producer consumer #{state.opts[:consumer_name]} was deleted; " <>
        "reconnecting"
    )

    unsub(state.conn_pid, state.sid)
    Process.demonitor(state.conn_ref, [:flush])
    send(self(), :connect)
    {:noreply, [], disconnected(state)}
  end

  defp handle_status(status, description, state) do
    interval = state.opts[:receive_interval]

    Logger.warning(
      "OffBroadway.Jetstream.Producer pull request rejected (#{status} #{description}); " <>
        "retrying in #{interval}ms"
    )

    timer = Process.send_after(self(), :pull, interval)
    {:noreply, [], %{state | pull: nil, timer: timer}}
  end

  ## Helpers

  defp disconnected(state) do
    %{state | status: :disconnected, conn_pid: nil, conn_ref: nil, sid: nil, pull: nil}
  end

  defp jetstream_metadata(msg) do
    case Gnat.Jetstream.API.Message.metadata(msg) do
      {:ok, metadata} -> metadata
      {:error, _reason} -> nil
    end
  end

  defp nack(conn, reply_to) do
    Gnat.Jetstream.nack(%{gnat: conn, reply_to: reply_to})
  catch
    :exit, _reason -> :ok
  end

  defp unsub(conn, sid) do
    Gnat.unsub(conn, sid)
  catch
    :exit, _reason -> :ok
  end

  defp cancel_timer(nil), do: :ok
  defp cancel_timer(timer), do: Process.cancel_timer(timer)
end
