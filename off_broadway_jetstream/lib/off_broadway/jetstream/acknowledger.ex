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

    ack_messages(successful, conn, :on_success, on_success)
    ack_messages(failed, conn, :on_failure, on_failure)
  end

  defp ack_messages(messages, conn, key, default) do
    Enum.each(messages, fn %Broadway.Message{acknowledger: {_, _, ack_data}} ->
      ack_data
      |> Map.get(key, default)
      |> perform(conn, ack_data.reply_to)
    end)
  end

  defp perform(:noop, _conn, _reply_to), do: :ok

  defp perform(action, conn, reply_to) do
    case send_ack(action, conn, reply_to) do
      :ok -> :ok
      {:error, reason} -> log_failure(action, reply_to, reason)
    end
  catch
    :exit, reason -> log_failure(action, reply_to, reason)
  end

  defp send_ack(:ack, conn, reply_to),
    do: Gnat.Jetstream.ack(%{gnat: conn, reply_to: reply_to})

  defp send_ack(:nack, conn, reply_to),
    do: Gnat.Jetstream.nack(%{gnat: conn, reply_to: reply_to})

  defp send_ack({:nack, delay_ms}, conn, reply_to),
    do: Gnat.pub(conn, reply_to, ~s(-NAK {"delay": #{delay_ms * 1_000_000}}))

  defp send_ack(:term, conn, reply_to),
    do: Gnat.Jetstream.ack_term(%{gnat: conn, reply_to: reply_to})

  defp log_failure(action, reply_to, reason) do
    Logger.warning(
      "OffBroadway.Jetstream.Acknowledger failed to #{inspect(action)} message #{reply_to}: " <>
        inspect(reason)
    )
  end

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
