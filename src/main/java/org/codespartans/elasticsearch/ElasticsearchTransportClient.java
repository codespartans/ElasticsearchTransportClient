package org.codespartans.elasticsearch;

import org.elasticsearch.action.*;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.explain.ExplainRequest;
import org.elasticsearch.action.explain.ExplainRequestBuilder;
import org.elasticsearch.action.explain.ExplainResponse;
import org.elasticsearch.action.fieldstats.FieldStatsRequest;
import org.elasticsearch.action.fieldstats.FieldStatsRequestBuilder;
import org.elasticsearch.action.fieldstats.FieldStatsResponse;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.*;
import org.elasticsearch.action.termvectors.*;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.threadpool.ThreadPool;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.AbstractMap.SimpleImmutableEntry;

/**
 * Class that wraps the Elasticsearch transport client to remove Elasticsearch configuration hassle.
 */
public class ElasticsearchTransportClient implements Client {

    private static final String ENV_HOSTS = "ELASTICSEARCH_HOSTS";
    private static final String ENV_CLUSTER_NAME = "ELASTICSEARCH_CLUSTER_NAME";
    public static final String DEFAULT_HOSTS = "localhost:9300";
    private static final String DEFAULT_CLUSTER_NAME = "elasticsearch";

    private final Client client;

    private ElasticsearchTransportClient(List<SimpleImmutableEntry<String, Integer>> hosts, String clusterName) throws UnknownHostException {
        validateHosts(hosts);
        Objects.requireNonNull(clusterName);

        Settings settings = Settings
                .builder()
                .put("cluster.name", clusterName)
                .build();

        TransportClient client = TransportClient
                .builder()
                .settings(settings)
                .build();

        for (SimpleImmutableEntry<String, Integer> host : hosts) {
            client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host.getKey()), host.getValue()));
        }

        this.client = client;
    }

    /**
     * Wrapped Elasticsearch transport client configured with environment variables.
     * Set the ELASTICSEARCH_HOSTS variable to indicate which hosts you would like to target.
     * Set the ELASTICSEARCH_CLUSTER_NAME variable to set the cluster name.
     *
     * @return ElasticsearchTransportClient
     * @throws UnknownHostException
     */
    public static ElasticsearchTransportClient getClient() throws UnknownHostException {
        return getClient(getHosts(), getClusterName());
    }

    /**
     * Wrapped Elasticsearch transport client with the default cluster name 'elasticsearch'.
     *
     * @param hosts The hosts you would like to target.
     * @return ElasticsearchTransportClient
     * @throws UnknownHostException
     */
    public static ElasticsearchTransportClient getClient(List<SimpleImmutableEntry<String, Integer>> hosts) throws UnknownHostException {
        return getClient(hosts, getClusterName());
    }

    /**
     * Wrapped Elasticsearch transport client configured by you.
     *
     * @param hosts       The hosts you would like to target.
     * @param clusterName The cluster name of your Elasticsearch cluster.
     * @return ElasticsearchTransportClient
     * @throws UnknownHostException
     */
    public static ElasticsearchTransportClient getClient(List<SimpleImmutableEntry<String, Integer>> hosts, String clusterName) throws UnknownHostException {
        Objects.requireNonNull(hosts);
        if (hosts.isEmpty()) throw new IllegalArgumentException("Hosts param can't be an empty list.");
        return new ElasticsearchTransportClient(hosts, clusterName);
    }

    private void validateHosts(List<SimpleImmutableEntry<String, Integer>> hosts) {
        hosts
                .stream()
                .map(Objects::requireNonNull)
                .forEach(kv -> {
                    Objects.requireNonNull(kv.getKey());
                    Objects.requireNonNull(kv.getValue());
                });
    }

    private static List<SimpleImmutableEntry<String, Integer>> getHosts() {
        String hosts = Optional
                .ofNullable(System.getenv(ENV_HOSTS))
                .orElse(DEFAULT_HOSTS);

        return Arrays
                .asList(hosts.split(","))
                .stream()
                .map(host -> {
                    String[] hostPortCombo = host.split(":");
                    return new SimpleImmutableEntry<>(hostPortCombo[0], Integer.parseInt(hostPortCombo[1]));
                })
                .collect(Collectors.toList());
    }

    private static String getClusterName() {
        return Optional
                .ofNullable(System.getenv(ENV_CLUSTER_NAME))
                .orElse(DEFAULT_CLUSTER_NAME);
    }

    /**
     * The admin client that can be used to perform administrative operations.
     */
    @Override
    public AdminClient admin() {
        return client.admin();
    }

    /**
     * Index a JSON source associated with a given index and type.
     * <p>
     * The id is optional, if it is not provided, one will be generated automatically.
     *
     * @param request The index request
     * @return The result future
     * @see Requests#indexRequest(String)
     */
    @Override
    public ActionFuture<IndexResponse> index(IndexRequest request) {
        return client.index(request);
    }

    /**
     * Index a document associated with a given index and type.
     * <p>
     * The id is optional, if it is not provided, one will be generated automatically.
     *
     * @param request  The index request
     * @param listener A listener to be notified with a result
     * @see Requests#indexRequest(String)
     */
    @Override
    public void index(IndexRequest request, ActionListener<IndexResponse> listener) {
        client.index(request, listener);
    }

    /**
     * Index a document associated with a given index and type.
     * <p>
     * The id is optional, if it is not provided, one will be generated automatically.
     */
    @Override
    public IndexRequestBuilder prepareIndex() {
        return client.prepareIndex();
    }

    /**
     * Updates a document based on a script.
     *
     * @param request The update request
     * @return The result future
     */
    @Override
    public ActionFuture<UpdateResponse> update(UpdateRequest request) {
        return client.update(request);
    }

    /**
     * Updates a document based on a script.
     *
     * @param request  The update request
     * @param listener A listener to be notified with a result
     */
    @Override
    public void update(UpdateRequest request, ActionListener<UpdateResponse> listener) {
        client.update(request, listener);
    }

    /**
     * Updates a document based on a script.
     */
    @Override
    public UpdateRequestBuilder prepareUpdate() {
        return client.prepareUpdate();
    }

    /**
     * Updates a document based on a script.
     *
     * @param index
     * @param type
     * @param id
     */
    @Override
    public UpdateRequestBuilder prepareUpdate(String index, String type, String id) {
        return client.prepareUpdate(index, type, id);
    }

    /**
     * Index a document associated with a given index and type.
     * <p>
     * The id is optional, if it is not provided, one will be generated automatically.
     *
     * @param index The index to index the document to
     * @param type  The type to index the document to
     */
    @Override
    public IndexRequestBuilder prepareIndex(String index, String type) {
        return client.prepareIndex(index, type);
    }

    /**
     * Index a document associated with a given index and type.
     * <p>
     * The id is optional, if it is not provided, one will be generated automatically.
     *
     * @param index The index to index the document to
     * @param type  The type to index the document to
     * @param id    The id of the document
     */
    @Override
    public IndexRequestBuilder prepareIndex(String index, String type, @Nullable String id) {
        return client.prepareIndex(index, type, id);
    }

    /**
     * Deletes a document from the index based on the index, type and id.
     *
     * @param request The delete request
     * @return The result future
     * @see Requests#deleteRequest(String)
     */
    @Override
    public ActionFuture<DeleteResponse> delete(DeleteRequest request) {
        return client.delete(request);
    }

    /**
     * Deletes a document from the index based on the index, type and id.
     *
     * @param request  The delete request
     * @param listener A listener to be notified with a result
     * @see Requests#deleteRequest(String)
     */
    @Override
    public void delete(DeleteRequest request, ActionListener<DeleteResponse> listener) {
        client.delete(request, listener);
    }

    /**
     * Deletes a document from the index based on the index, type and id.
     */
    @Override
    public DeleteRequestBuilder prepareDelete() {
        return client.prepareDelete();
    }

    /**
     * Deletes a document from the index based on the index, type and id.
     *
     * @param index The index to delete the document from
     * @param type  The type of the document to delete
     * @param id    The id of the document to delete
     */
    @Override
    public DeleteRequestBuilder prepareDelete(String index, String type, String id) {
        return client.prepareDelete(index, type, id);
    }

    /**
     * Executes a bulk of index / delete operations.
     *
     * @param request The bulk request
     * @return The result future
     * @see Requests#bulkRequest()
     */
    @Override
    public ActionFuture<BulkResponse> bulk(BulkRequest request) {
        return client.bulk(request);
    }

    /**
     * Executes a bulk of index / delete operations.
     *
     * @param request  The bulk request
     * @param listener A listener to be notified with a result
     * @see Requests#bulkRequest()
     */
    @Override
    public void bulk(BulkRequest request, ActionListener<BulkResponse> listener) {
        client.bulk(request, listener);
    }

    /**
     * Executes a bulk of index / delete operations.
     */
    @Override
    public BulkRequestBuilder prepareBulk() {
        return prepareBulk();
    }

    /**
     * Gets the document that was indexed from an index with a type and id.
     *
     * @param request The get request
     * @return The result future
     * @see Requests#getRequest(String)
     */
    @Override
    public ActionFuture<GetResponse> get(GetRequest request) {
        return client.get(request);
    }

    /**
     * Gets the document that was indexed from an index with a type and id.
     *
     * @param request  The get request
     * @param listener A listener to be notified with a result
     * @see Requests#getRequest(String)
     */
    @Override
    public void get(GetRequest request, ActionListener<GetResponse> listener) {
        client.get(request, listener);
    }

    /**
     * Gets the document that was indexed from an index with a type and id.
     */
    @Override
    public GetRequestBuilder prepareGet() {
        return client.prepareGet();
    }

    /**
     * Gets the document that was indexed from an index with a type (optional) and id.
     *
     * @param index
     * @param type
     * @param id
     */
    @Override
    public GetRequestBuilder prepareGet(String index, @Nullable String type, String id) {
        return client.prepareGet(index, type, id);
    }

    /**
     * Multi get documents.
     *
     * @param request
     */
    @Override
    public ActionFuture<MultiGetResponse> multiGet(MultiGetRequest request) {
        return client.multiGet(request);
    }

    /**
     * Multi get documents.
     *
     * @param request
     * @param listener
     */
    @Override
    public void multiGet(MultiGetRequest request, ActionListener<MultiGetResponse> listener) {
        client.multiGet(request, listener);
    }

    /**
     * Multi get documents.
     */
    @Override
    public MultiGetRequestBuilder prepareMultiGet() {
        return client.prepareMultiGet();
    }

    /**
     * Search across one or more indices and one or more types with a query.
     *
     * @param request The search request
     * @return The result future
     * @see Requests#searchRequest(String...)
     */
    @Override
    public ActionFuture<SearchResponse> search(SearchRequest request) {
        return client.search(request);
    }

    /**
     * Search across one or more indices and one or more types with a query.
     *
     * @param request  The search request
     * @param listener A listener to be notified of the result
     * @see Requests#searchRequest(String...)
     */
    @Override
    public void search(SearchRequest request, ActionListener<SearchResponse> listener) {
        client.search(request, listener);
    }

    /**
     * Search across one or more indices and one or more types with a query.
     *
     * @param indices
     */
    @Override
    public SearchRequestBuilder prepareSearch(String... indices) {
        return client.prepareSearch(indices);
    }

    /**
     * A search scroll request to continue searching a previous scrollable search request.
     *
     * @param request The search scroll request
     * @return The result future
     * @see Requests#searchScrollRequest(String)
     */
    @Override
    public ActionFuture<SearchResponse> searchScroll(SearchScrollRequest request) {
        return client.searchScroll(request);
    }

    /**
     * A search scroll request to continue searching a previous scrollable search request.
     *
     * @param request  The search scroll request
     * @param listener A listener to be notified of the result
     * @see Requests#searchScrollRequest(String)
     */
    @Override
    public void searchScroll(SearchScrollRequest request, ActionListener<SearchResponse> listener) {
        client.searchScroll(request, listener);
    }

    /**
     * A search scroll request to continue searching a previous scrollable search request.
     *
     * @param scrollId
     */
    @Override
    public SearchScrollRequestBuilder prepareSearchScroll(String scrollId) {
        return client.prepareSearchScroll(scrollId);
    }

    /**
     * Performs multiple search requests.
     *
     * @param request
     */
    @Override
    public ActionFuture<MultiSearchResponse> multiSearch(MultiSearchRequest request) {
        return client.multiSearch(request);
    }

    /**
     * Performs multiple search requests.
     *
     * @param request
     * @param listener
     */
    @Override
    public void multiSearch(MultiSearchRequest request, ActionListener<MultiSearchResponse> listener) {
        client.multiSearch(request, listener);
    }

    /**
     * Performs multiple search requests.
     */
    @Override
    public MultiSearchRequestBuilder prepareMultiSearch() {
        return client.prepareMultiSearch();
    }

    /**
     * An action that returns the term vectors for a specific document.
     *
     * @param request The term vector request
     * @return The response future
     */
    @Override
    public ActionFuture<TermVectorsResponse> termVectors(TermVectorsRequest request) {
        return client.termVectors(request);
    }

    /**
     * An action that returns the term vectors for a specific document.
     *
     * @param request  The term vector request
     * @param listener
     */
    @Override
    public void termVectors(TermVectorsRequest request, ActionListener<TermVectorsResponse> listener) {
        client.termVectors(request, listener);
    }

    /**
     * Builder for the term vector request.
     */
    @Override
    public TermVectorsRequestBuilder prepareTermVectors() {
        return client.prepareTermVectors();
    }

    /**
     * Builder for the term vector request.
     *
     * @param index The index to load the document from
     * @param type  The type of the document
     * @param id    The id of the document
     */
    @Override
    public TermVectorsRequestBuilder prepareTermVectors(String index, String type, String id) {
        return client.prepareTermVectors(index, type, id);
    }

    /**
     * An action that returns the term vectors for a specific document.
     *
     * @param request The term vector request
     * @return The response future
     */
    @Deprecated
    @Override
    public ActionFuture<TermVectorsResponse> termVector(TermVectorsRequest request) {
        return client.termVector(request);
    }

    /**
     * An action that returns the term vectors for a specific document.
     *
     * @param request  The term vector request
     * @param listener
     */
    @Override
    public void termVector(TermVectorsRequest request, ActionListener<TermVectorsResponse> listener) {
        client.termVector(request, listener);
    }

    /**
     * Builder for the term vector request.
     */
    @Deprecated
    @Override
    public TermVectorsRequestBuilder prepareTermVector() {
        return client.prepareTermVector();
    }

    /**
     * Builder for the term vector request.
     *
     * @param index The index to load the document from
     * @param type  The type of the document
     * @param id    The id of the document
     */
    @Deprecated
    @Override
    public TermVectorsRequestBuilder prepareTermVector(String index, String type, String id) {
        return client.prepareTermVector(index, type, id);
    }

    /**
     * Multi get term vectors.
     *
     * @param request
     */
    @Override
    public ActionFuture<MultiTermVectorsResponse> multiTermVectors(MultiTermVectorsRequest request) {
        return client.multiTermVectors(request);
    }

    /**
     * Multi get term vectors.
     *
     * @param request
     * @param listener
     */
    @Override
    public void multiTermVectors(MultiTermVectorsRequest request, ActionListener<MultiTermVectorsResponse> listener) {
        client.multiTermVectors(request, listener);
    }

    /**
     * Multi get term vectors.
     */
    @Override
    public MultiTermVectorsRequestBuilder prepareMultiTermVectors() {
        return client.prepareMultiTermVectors();
    }

    /**
     * Computes a score explanation for the specified request.
     *
     * @param index The index this explain is targeted for
     * @param type  The type this explain is targeted for
     * @param id    The document identifier this explain is targeted for
     */
    @Override
    public ExplainRequestBuilder prepareExplain(String index, String type, String id) {
        return client.prepareExplain(index, type, id);
    }

    /**
     * Computes a score explanation for the specified request.
     *
     * @param request The request encapsulating the query and document identifier to compute a score explanation for
     */
    @Override
    public ActionFuture<ExplainResponse> explain(ExplainRequest request) {
        return client.explain(request);
    }

    /**
     * Computes a score explanation for the specified request.
     *
     * @param request  The request encapsulating the query and document identifier to compute a score explanation for
     * @param listener A listener to be notified of the result
     */
    @Override
    public void explain(ExplainRequest request, ActionListener<ExplainResponse> listener) {
        client.explain(request, listener);
    }

    /**
     * Clears the search contexts associated with specified scroll ids.
     */
    @Override
    public ClearScrollRequestBuilder prepareClearScroll() {
        return client.prepareClearScroll();
    }

    /**
     * Clears the search contexts associated with specified scroll ids.
     *
     * @param request
     */
    @Override
    public ActionFuture<ClearScrollResponse> clearScroll(ClearScrollRequest request) {
        return client.clearScroll(request);
    }

    /**
     * Clears the search contexts associated with specified scroll ids.
     *
     * @param request
     * @param listener
     */
    @Override
    public void clearScroll(ClearScrollRequest request, ActionListener<ClearScrollResponse> listener) {
        client.clearScroll(request, listener);
    }

    @Override
    public FieldStatsRequestBuilder prepareFieldStats() {
        return client.prepareFieldStats();
    }

    @Override
    public ActionFuture<FieldStatsResponse> fieldStats(FieldStatsRequest request) {
        return client.fieldStats(request);
    }

    @Override
    public void fieldStats(FieldStatsRequest request, ActionListener<FieldStatsResponse> listener) {
        client.fieldStats(request, listener);
    }

    /**
     * Returns this clients settings
     */
    @Override
    public Settings settings() {
        return client.settings();
    }

    /**
     * Returns a new lightweight Client that applies all given headers to each of the requests
     * issued from it.
     *
     * @param headers
     */
    @Override
    public Client filterWithHeader(Map<String, String> headers) {
        return client.filterWithHeader(headers);
    }

    /**
     * Executes a generic action, denoted by an {@link Action}.
     *
     * @param action  The action type to execute.
     * @param request The action request.
     * @return A future allowing to get back the response.
     */
    @Override
    public <Request extends ActionRequest<Request>, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> ActionFuture<Response> execute(Action<Request, Response, RequestBuilder> action, Request request) {
        return client.execute(action, request);
    }

    /**
     * Executes a generic action, denoted by an {@link Action}.
     *
     * @param action   The action type to execute.
     * @param request  The action request.
     * @param listener The listener to receive the response back.
     */
    @Override
    public <Request extends ActionRequest<Request>, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> void execute(Action<Request, Response, RequestBuilder> action, Request request, ActionListener<Response> listener) {
        client.execute(action, request, listener);
    }

    /**
     * Prepares a request builder to execute, specified by {@link Action}.
     *
     * @param action The action type to execute.
     * @return The request builder, that can, at a later stage, execute the request.
     */
    @Override
    public <Request extends ActionRequest<Request>, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> RequestBuilder prepareExecute(Action<Request, Response, RequestBuilder> action) {
        return client.prepareExecute(action);
    }

    /**
     * Returns the threadpool used to execute requests on this client
     */
    @Override
    public ThreadPool threadPool() {
        return client.threadPool();
    }

    @Override
    public void close() {
        client.close();
    }
}
