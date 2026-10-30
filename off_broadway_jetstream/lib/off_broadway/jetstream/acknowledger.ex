defmodule OffBroadway.Jetstream.Acknowledger do
  @moduledoc false

  @behaviour Broadway.Acknowledger

  require Logger

  alias OffBroadway.Jetstream.Options

  @type ack_ref :: {module(), Broadway.name()}

  @doc false
  @spec put_config(ack_ref(), map()) :: :ok
  def put_config(ack_ref, config), do: :persistent_term.put(ack_ref, config)

  @doc false
  @spec config(ack_ref()) :: map()
  def config(ack_ref), do: :persistent_term.get(ack_ref)

  @impl Broadway.Acknowledger
  def ack(ack_ref, successful, failed) do
    %{connection_name: conn, on_success: on_success, on_failure: on_failure} = config(ack_ref)

    actions =
      Enum.map(successful, &action(&1, :on_success, on_success)) ++
        Enum.map(failed, &action(&1, :on_failure, on_failure))

    actions = Enum.reject(actions, &match?({:noop, _reply_to}, &1))

    # Stop at the first exit: the connection is gone or not responding, so every further
    # call would fail too or block for its full timeout. JetStream redelivers the skipped
    # messages once their ack_wait elapses.
    Enum.reduce_while(actions, length(actions), fn {action, reply_to}, remaining ->
      case perform(action, conn, reply_to) do
        :ok ->
          {:cont, remaining - 1}

        {:exit, reason} ->
          Logger.warning(
            "OffBroadway.Jetstream.Acknowledger could not reach Gnat connection " <>
              "#{inspect(conn)} (#{inspect(reason)}); skipped #{remaining} " <>
              "acknowledgement(s), JetStream redelivers those messages after ack_wait"
          )

          {:halt, remaining}
      end
    end)

    :ok
  end

  defp action(%Broadway.Message{acknowledger: {_, _, ack_data}}, key, default),
    do: {Map.get(ack_data, key, default), ack_data.reply_to}

  defp perform(action, conn, reply_to) do
    case send_ack(action, conn, reply_to) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.warning(
          "OffBroadway.Jetstream.Acknowledger failed to #{inspect(action)} message " <>
            "#{reply_to}: #{inspect(reason)}"
        )
    end
  catch
    :exit, reason -> {:exit, reason}
  end

  defp send_ack(:ack, conn, reply_to),
    do: Gnat.Jetstream.ack(%{gnat: conn, reply_to: reply_to})

  defp send_ack(:nack, conn, reply_to),
    do: Gnat.Jetstream.nack(%{gnat: conn, reply_to: reply_to})

  defp send_ack({:nack, delay_ms}, conn, reply_to),
    do: Gnat.pub(conn, reply_to, ~s(-NAK {"delay": #{delay_ms * 1_000_000}}))

  defp send_ack(:term, conn, reply_to),
    do: Gnat.Jetstream.ack_term(%{gnat: conn, reply_to: reply_to})

  @impl Broadway.Acknowledger
  def configure(_ack_ref, ack_data, options) do
    Enum.each(options, fn
      {key, value} when key in [:on_success, :on_failure] ->
        with {:error, message} <- Options.validate_ack_action(value) do
          raise ArgumentError, "invalid #{inspect(key)} given to configure_ack: #{message}"
        end

      {key, _value} ->
        raise ArgumentError, "unsupported configure_ack option #{inspect(key)}"
    end)

    {:ok, Map.merge(ack_data, Map.new(options))}
  end
end
