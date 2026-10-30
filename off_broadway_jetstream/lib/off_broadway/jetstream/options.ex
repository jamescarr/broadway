defmodule OffBroadway.Jetstream.Options do
  @moduledoc false

  alias Gnat.Jetstream.API.Consumer

  @ack_action_error "expected one of :ack, :nack, :term, :noop or {:nack, delay_ms}"

  @definition NimbleOptions.new!(
                connection_name: [
                  type: {:or, [:atom, :pid]},
                  required: true,
                  doc: """
                  The registered name of the `Gnat` connection, usually the `:name` given to
                  `Gnat.ConnectionSupervisor`. A pid is accepted too, but it becomes stale as soon
                  as the connection is restarted, so prefer a name.
                  """
                ],
                stream_name: [
                  type: :string,
                  doc: """
                  The name of the stream to consume from. Required unless `:consumer` is given.
                  """
                ],
                consumer_name: [
                  type: :string,
                  doc: """
                  The name of the durable pull consumer to fetch messages from. Required unless
                  `:consumer` is given.
                  """
                ],
                consumer: [
                  type: {:struct, Consumer},
                  doc: """
                  A `Gnat.Jetstream.API.Consumer` definition. When given, every producer creates
                  the durable consumer on connect if it does not exist yet (an existing consumer
                  is used as is). The stream name, consumer name and domain are taken from the
                  struct, so this option cannot be combined with `:stream_name`, `:consumer_name`
                  or `:domain`.
                  """
                ],
                domain: [
                  type: :string,
                  doc: "The JetStream domain of the stream."
                ],
                inbox_prefix: [
                  type: :string,
                  default: "_INBOX.",
                  doc: "The prefix of the subject each producer receives its messages on."
                ],
                connection_retries: [
                  type: :non_neg_integer,
                  default: 10,
                  doc: """
                  How many times the producer retries to find the `Gnat` connection or to reach
                  the consumer before it stops.
                  """
                ],
                connection_retry_timeout: [
                  type: :non_neg_integer,
                  default: 1_000,
                  doc: "The time in milliseconds between those retries."
                ],
                request_expires: [
                  type: :pos_integer,
                  default: 5_000,
                  doc: """
                  The time in milliseconds a pull request stays open on the server while waiting
                  for messages. When it expires, the producer immediately issues a new one.
                  """
                ],
                receive_interval: [
                  type: :non_neg_integer,
                  default: 5_000,
                  doc: """
                  The time in milliseconds to wait before pulling again after the server rejected
                  a pull request.
                  """
                ],
                max_batch_size: [
                  type: :pos_integer,
                  default: 100,
                  doc: """
                  The maximum number of messages requested by a single pull request. The producer
                  requests `min(demand, max_batch_size)` messages at a time.
                  """
                ],
                on_success: [
                  type: {:custom, __MODULE__, :validate_ack_action, []},
                  default: :ack,
                  doc: """
                  The acknowledgement action for successful messages. See the
                  "Acknowledgements" section below.
                  """
                ],
                on_failure: [
                  type: {:custom, __MODULE__, :validate_ack_action, []},
                  default: :nack,
                  doc: """
                  The acknowledgement action for failed messages. See the
                  "Acknowledgements" section below.
                  """
                ]
              )

  @doc false
  def definition, do: @definition

  @doc false
  def validate_ack_action(action)
      when action in [:ack, :nack, :term, :noop],
      do: {:ok, action}

  def validate_ack_action({:nack, delay_ms} = action)
      when is_integer(delay_ms) and delay_ms >= 0,
      do: {:ok, action}

  def validate_ack_action(other),
    do: {:error, "#{@ack_action_error}, got: #{inspect(other)}"}

  @doc false
  def validate!(opts) do
    opts
    |> NimbleOptions.validate!(@definition)
    |> validate_consumer!()
  end

  defp validate_consumer!(opts) do
    case Keyword.fetch(opts, :consumer) do
      {:ok, consumer} ->
        cond do
          Enum.any?([:stream_name, :consumer_name, :domain], &Keyword.has_key?(opts, &1)) ->
            raise ArgumentError,
                  ":consumer cannot be combined with :stream_name, :consumer_name or :domain"

          consumer.stream_name == nil ->
            raise ArgumentError, ":consumer must have :stream_name set"

          consumer.durable_name == nil ->
            raise ArgumentError,
                  ":consumer must have :durable_name set; " <>
                    "ephemeral consumers cannot be shared between producers"

          consumer.ack_policy != :explicit ->
            raise ArgumentError, ":consumer must use ack_policy: :explicit"

          true ->
            Keyword.merge(opts,
              stream_name: consumer.stream_name,
              consumer_name: consumer.durable_name,
              domain: consumer.domain
            )
        end

      :error ->
        if Keyword.has_key?(opts, :stream_name) and Keyword.has_key?(opts, :consumer_name) do
          opts
        else
          raise ArgumentError,
                "either :consumer or both :stream_name and :consumer_name are required"
        end
    end
  end
end
