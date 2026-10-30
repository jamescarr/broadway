defmodule OffBroadway.Jetstream.ProducerTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Broadway.Message
  alias Gnat.Jetstream.API.{Consumer, Stream}
  alias OffBroadway.Jetstream.Producer

  defmodule Forwarder do
    use Broadway

    def start_link(opts), do: Broadway.start_link(__MODULE__, opts)

    @impl true
    def handle_message(_processor, message, %{test_pid: test_pid}) do
      send(test_pid, {:message_handled, message})

      case message.data do
        "fail" <> _ -> Message.failed(message, "boom")
        "noop" <> _ -> Message.configure_ack(message, on_success: :noop)
        _ -> message
      end
    end
  end

  setup do
    n = System.unique_integer([:positive])
    stream = "OBJ_TEST_#{n}"
    subject = "obj.test.#{n}"

    {:ok, _} = Stream.create(:gnat, %Stream{name: stream, subjects: [subject]})
    # Deleting the stream deletes its consumers too.
    on_exit(fn -> Stream.delete(:gnat, stream) end)

    %{stream: stream, subject: subject}
  end

  test "delivers messages with data, subject, headers and jetstream metadata", ctx do
    %{stream: stream, subject: subject} = ctx
    create_consumer(stream, "worker")
    start_pipeline(stream_name: stream, consumer_name: "worker")

    for i <- 1..5 do
      :ok = Gnat.pub(:gnat, subject, "message #{i}", headers: [{"x-test", "#{i}"}])
    end

    for i <- 1..5 do
      data = "message #{i}"
      header = "#{i}"

      assert_receive {:message_handled,
                      %Message{
                        data: ^data,
                        metadata: %{
                          subject: ^subject,
                          headers: [{"x-test", ^header}],
                          reply_to: "$JS.ACK." <> _,
                          jetstream: %Gnat.Jetstream.API.Message.Metadata{
                            stream: ^stream,
                            consumer: "worker",
                            num_delivered: 1
                          }
                        }
                      }}
    end
  end

  test "keeps pulling while there are more messages than demand", ctx do
    %{stream: stream, subject: subject} = ctx
    create_consumer(stream, "worker")
    start_pipeline(stream_name: stream, consumer_name: "worker")

    published = for i <- 1..250, do: "message #{i}"
    Enum.each(published, &(:ok = Gnat.pub(:gnat, subject, &1)))

    received =
      for _ <- published do
        assert_receive {:message_handled, %Message{data: data}}, 5_000
        data
      end

    assert Enum.sort(received) == Enum.sort(published)
    refute_receive {:message_handled, _}, 300
  end

  test "acks successful messages", ctx do
    %{stream: stream, subject: subject} = ctx
    create_consumer(stream, "worker")
    start_pipeline(stream_name: stream, consumer_name: "worker")

    for i <- 1..3, do: :ok = Gnat.pub(:gnat, subject, "message #{i}")
    for _ <- 1..3, do: assert_receive({:message_handled, _})

    eventually(fn ->
      match?(
        {:ok, %{num_ack_pending: 0, num_pending: 0}},
        Consumer.info(:gnat, stream, "worker")
      )
    end)
  end

  test "nacks failed messages by default so they are redelivered", ctx do
    %{stream: stream, subject: subject} = ctx
    create_consumer(stream, "worker", max_deliver: 3)
    start_pipeline(stream_name: stream, consumer_name: "worker")

    :ok = Gnat.pub(:gnat, subject, "fail")

    for n <- 1..3 do
      assert_receive {:message_handled,
                      %Message{data: "fail", metadata: %{jetstream: %{num_delivered: ^n}}}}
    end

    refute_receive {:message_handled, %Message{data: "fail"}}, 500
  end

  test "on_failure: :term stops redelivery", ctx do
    %{stream: stream, subject: subject} = ctx
    create_consumer(stream, "worker")
    start_pipeline(stream_name: stream, consumer_name: "worker", on_failure: :term)

    :ok = Gnat.pub(:gnat, subject, "fail")

    assert_receive {:message_handled, %Message{data: "fail"}}
    refute_receive {:message_handled, %Message{data: "fail"}}, 500

    eventually(fn ->
      match?({:ok, %{num_ack_pending: 0}}, Consumer.info(:gnat, stream, "worker"))
    end)
  end

  test "on_failure: {:nack, delay} delays redelivery", ctx do
    %{stream: stream, subject: subject} = ctx
    create_consumer(stream, "worker")
    start_pipeline(stream_name: stream, consumer_name: "worker", on_failure: {:nack, 500})

    :ok = Gnat.pub(:gnat, subject, "fail")

    assert_receive {:message_handled,
                    %Message{data: "fail", metadata: %{jetstream: %{num_delivered: 1}}}}

    refute_receive {:message_handled, %Message{metadata: %{jetstream: %{num_delivered: 2}}}},
                   300

    assert_receive {:message_handled,
                    %Message{data: "fail", metadata: %{jetstream: %{num_delivered: 2}}}}
  end

  test "Message.configure_ack/2 overrides the action per message", ctx do
    %{stream: stream, subject: subject} = ctx
    create_consumer(stream, "worker", ack_wait: 1_000_000_000)
    start_pipeline(stream_name: stream, consumer_name: "worker")

    :ok = Gnat.pub(:gnat, subject, "noop")
    :ok = Gnat.pub(:gnat, subject, "ok")

    assert_receive {:message_handled, %Message{data: "noop"}}
    assert_receive {:message_handled, %Message{data: "ok"}}

    eventually(fn ->
      match?({:ok, %{num_ack_pending: 1}}, Consumer.info(:gnat, stream, "worker"))
    end)

    assert_receive {:message_handled,
                    %Message{data: "noop", metadata: %{jetstream: %{num_delivered: 2}}}},
                   3_000

    refute_received {:message_handled, %Message{data: "ok"}}
  end

  test "creates the consumer from :consumer when missing and is idempotent across producers",
       ctx do
    %{stream: stream, subject: subject} = ctx

    start_pipeline([consumer: %Consumer{stream_name: stream, durable_name: "auto"}],
      producer: [concurrency: 3]
    )

    eventually(fn -> match?({:ok, _}, Consumer.info(:gnat, stream, "auto")) end)

    published = for i <- 1..5, do: "message #{i}"
    Enum.each(published, &(:ok = Gnat.pub(:gnat, subject, &1)))

    received =
      for _ <- published do
        assert_receive {:message_handled, %Message{data: data}}
        data
      end

    assert Enum.sort(received) == Enum.sort(published)
    refute_receive {:message_handled, _}, 300
  end

  test "stops with an error when the consumer does not exist", %{stream: stream} do
    Process.flag(:trap_exit, true)

    log =
      capture_log(fn ->
        {:ok, pipeline} =
          Forwarder.start_link(
            pipeline_opts(stream_name: stream, consumer_name: "missing", connection_retries: 0)
          )

        assert_receive {:EXIT, ^pipeline, _reason}, 5_000
      end)

    assert log =~ "could not use consumer missing on stream #{stream}"
    assert log =~ "consumer not found"
  end

  test "returns an error for invalid options", %{stream: stream} do
    Process.flag(:trap_exit, true)

    nimble_error =
      "invalid configuration given to OffBroadway.Jetstream.Producer.prepare_for_start/2"

    cases = [
      {[connection_name: :gnat],
       "either :consumer or both :stream_name and :consumer_name are required"},
      {[stream_name: stream, consumer_name: "w"],
       nimble_error <> ", required :connection_name option not found"},
      {[connection_name: :gnat, consumer: %Consumer{stream_name: stream}],
       ":consumer must have :durable_name set; " <>
         "ephemeral consumers cannot be shared between producers"},
      {[connection_name: :gnat, consumer: %Consumer{stream_name: nil, durable_name: "w"}],
       ":consumer must have :stream_name set"},
      {[
         connection_name: :gnat,
         stream_name: stream,
         consumer: %Consumer{stream_name: stream, durable_name: "w"}
       ], ":consumer cannot be combined with :stream_name, :consumer_name or :domain"},
      {[
         connection_name: :gnat,
         consumer: %Consumer{stream_name: stream, durable_name: "w", ack_policy: :all}
       ], ":consumer must use ack_policy: :explicit"},
      {[connection_name: :gnat, stream_name: stream, consumer_name: "w", on_failure: :bogus],
       nimble_error <>
         ", invalid value for :on_failure option: " <>
         "expected one of :ack, :nack, :term, :noop or {:nack, delay_ms}, got: :bogus"},
      {[
         connection_name: :gnat,
         stream_name: stream,
         consumer_name: "w",
         on_failure: {:nack, -1}
       ], "got: {:nack, -1}"}
    ]

    for {producer_opts, expected} <- cases do
      opts =
        pipeline_opts([])
        |> Keyword.put(:producer, module: {Producer, producer_opts})

      assert {:error, {%ArgumentError{message: message}, _stacktrace}} =
               Forwarder.start_link(opts)

      assert message =~ expected
    end
  end

  test "stops pulling after draining", ctx do
    %{stream: stream, subject: subject} = ctx
    create_consumer(stream, "worker")

    name =
      start_pipeline(stream_name: stream, consumer_name: "worker", request_expires: 30_000)

    # The producer has an outstanding pull request.
    eventually(fn -> match?({:ok, %{num_waiting: 1}}, Consumer.info(:gnat, stream, "worker")) end)

    [producer] = Broadway.producer_names(name)
    Broadway.Topology.ProducerStage.drain(producer)
    :sys.get_state(producer)

    :ok = Gnat.pub(:gnat, subject, "after drain")
    refute_receive {:message_handled, %Message{data: "after drain"}}, 500

    # The message was not delivered and the pull request is gone, long before it expires.
    eventually(
      fn ->
        match?(
          {:ok, %{num_pending: 1, num_ack_pending: 0, num_waiting: 0}},
          Consumer.info(:gnat, stream, "worker")
        )
      end,
      5_000
    )
  end

  test "reconnects after the Gnat connection restarts", ctx do
    %{stream: stream, subject: subject} = ctx
    create_consumer(stream, "worker")
    start_pipeline(stream_name: stream, consumer_name: "worker", connection_retry_timeout: 100)

    eventually(fn -> match?({:ok, %{num_waiting: 1}}, Consumer.info(:gnat, stream, "worker")) end)

    old = Process.whereis(:gnat)
    Process.exit(old, :kill)

    eventually(
      fn ->
        pid = Process.whereis(:gnat)
        pid != nil and pid != old
      end,
      5_000
    )

    :ok = Gnat.pub(:gnat, subject, "after reconnect")
    assert_receive {:message_handled, %Message{data: "after reconnect"}}, 5_000
  end

  ## Helpers

  defp create_consumer(stream, name, overrides \\ []) do
    consumer = struct(%Consumer{stream_name: stream, durable_name: name}, overrides)
    {:ok, _} = Consumer.create(:gnat, consumer)
  end

  defp pipeline_opts(producer_opts, overrides \\ []) do
    defaults = [
      name: :"jetstream_pipeline_#{System.unique_integer([:positive])}",
      context: %{test_pid: self()},
      producer: [
        module: {Producer, Keyword.put_new(producer_opts, :connection_name, :gnat)},
        concurrency: 1
      ],
      processors: [default: [concurrency: 1, max_demand: 10]]
    ]

    Keyword.merge(defaults, overrides, fn
      :producer, default, override -> Keyword.merge(default, override)
      _key, _default, override -> override
    end)
  end

  defp start_pipeline(producer_opts, overrides \\ []) do
    opts = pipeline_opts(producer_opts, overrides)
    start_supervised!({Forwarder, opts})
    opts[:name]
  end

  defp eventually(fun, timeout \\ 2_000) do
    cond do
      fun.() ->
        :ok

      timeout <= 0 ->
        flunk("condition was not met in time")

      true ->
        Process.sleep(50)
        eventually(fun, timeout - 50)
    end
  end
end
