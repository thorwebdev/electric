defmodule Electric.Replication.Postgres.ServerState do
  @moduledoc """
  Holds state information about a postgres db instance, stored in tables within the db itself.
  """
  use GenServer

  alias Electric.Replication.Connectors

  require Logger

  @type state() :: term()
  @callback connect(Connectors.config(), Keyword.t()) :: {:ok, state()}
  @callback load(state()) :: {:ok, Electric.Postgres.Schema.t() | nil}
  @callback save(state(), binary(), Electric.Postgres.Schema.t()) :: {:ok, state()}

  @spec name(Connectors.config()) :: Electric.reg_name()
  def name(conn_config) when is_list(conn_config) do
    name(Connectors.origin(conn_config))
  end

  @spec name(Connectors.origin()) :: Electric.reg_name()
  def name(origin) when is_binary(origin) do
    Electric.name(__MODULE__, origin)
  end

  def start_link({conn_config, opts}) do
    start_link(conn_config, opts)
  end

  def start_link(conn_config, opts \\ []) do
    GenServer.start_link(__MODULE__, {conn_config, opts}, name: name(conn_config))
  end

  def load(origin) do
    origin
    |> name()
    |> GenServer.call(:load)
  end

  def load!(origin) do
    case load(origin) do
      {:ok, schema} ->
        schema

      error ->
        raise RuntimeError, message: "Unable to load schema: #{inspect(error)}"
    end
  end

  def save(origin, version, schema) do
    origin
    |> name()
    |> GenServer.call({:save, version, schema})
  end

  @impl true
  def init({conn_config, opts}) do
    origin = Connectors.origin(conn_config)

    {backend_impl, backend_opts} =
      case Keyword.get(opts, :backend, {__MODULE__.Epgsql, []}) do
        module when is_atom(module) ->
          {module, []}

        {module, opts} when is_atom(module) and is_list(opts) ->
          {module, opts}
      end

    Logger.metadata(pg_producer: origin)
    Logger.info("Starting #{__MODULE__} using #{backend_impl} backend")

    {:ok, backend} = backend_impl.connect(conn_config, backend_opts)

    {:ok, %{backend: backend, backend_impl: backend_impl, opts: opts}}
  end

  @impl true
  def handle_call(:load, _from, state) do
    %{backend: backend, backend_impl: backend_impl} = state
    {:ok, schema} = backend_impl.load(backend)
    {:reply, {:ok, schema || Electric.Postgres.Schema.new()}, state}
  end

  def handle_call({:save, version, schema}, _from, state) do
    %{backend: backend, backend_impl: backend_impl} = state
    {:ok, backend} = backend_impl.save(backend, version, schema)
    {:reply, :ok, %{state | backend: backend}}
  end
end

defmodule Electric.Replication.Postgres.ServerState.Epgsql do
  alias Electric.Postgres.Extension
  alias Electric.Replication.Connectors

  @behaviour Electric.Replication.Postgres.ServerState

  @impl true
  def connect(conn_config, _opts) do
    conn_config
    |> Connectors.get_connection_opts(replication: false)
    |> :epgsql.connect()
  end

  @impl true
  def load(conn) do
    Extension.current_schema(conn)
  end

  @impl true
  def save(conn, version, schema) do
    with :ok <- Extension.save_schema(conn, version, schema) do
      {:ok, conn}
    end
  end
end
