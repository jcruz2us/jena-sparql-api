package org.aksw.jena_sparql_api.example.proxy;

import java.sql.SQLException;
import java.util.Collection;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;

import org.aksw.jena_sparql_api.cache.core.QueryExecutionFactoryCacheEx;
import org.aksw.jena_sparql_api.cache.extra.CacheBackend;
import org.aksw.jena_sparql_api.cache.extra.CacheFrontend;
import org.aksw.jena_sparql_api.cache.extra.CacheFrontendImpl;
import org.aksw.jena_sparql_api.cache.h2.CacheCoreH2;
import org.aksw.jena_sparql_api.core.QueryExecutionFactory;
import org.aksw.jena_sparql_api.http.QueryExecutionFactoryHttp;
import org.aksw.jena_sparql_api.utils.UriUtils;
import org.aksw.jena_sparql_api.web.SparqlEndpointBase;

import com.google.common.collect.Multimap;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;



@Path("/sparql")
public class SparqlEndpointProxy
	extends SparqlEndpointBase
{
	private String defaultServiceUri;
	private boolean allowOverrideServiceUri = false;
	
	public SparqlEndpointProxy(@Context ServletContext context) {
		
		this.defaultServiceUri = (String)context.getAttribute("defaultServiceUri");
		
		Boolean tmp = (Boolean)context.getAttribute("allowOverrideServiceUri");
		this.allowOverrideServiceUri = tmp == null ? true : tmp; 
		
		if(!allowOverrideServiceUri && (defaultServiceUri == null || defaultServiceUri.isEmpty()) ) {
			throw new RuntimeException("Overriding of service URI disabled, but no default URI set.");
		}
	}
	
	@Override
	public QueryExecution createQueryExecution(final Query query, @Context HttpServletRequest req) {
		
		Multimap<String, String> qs = UriUtils.parseQueryString(req.getQueryString());
		
		Collection<String> serviceUris = qs.get("service-uri");
		String serviceUri;
		if(serviceUris == null || serviceUris.isEmpty()) {
			serviceUri = defaultServiceUri;
		} else {
			serviceUri = serviceUris.iterator().next();
			
			// If overriding is disabled, a given uri must match the default one
			if(!allowOverrideServiceUri && !defaultServiceUri.equals(serviceUri)) {
				throw new RuntimeException("Access to any service other than " + defaultServiceUri + " is blocked.");
			}
		}
		
		if(serviceUri == null) {
			throw new RuntimeException("No SPARQL service URI sent with the request and no default one is configured");
		}

		
		QueryExecutionFactory qef = new QueryExecutionFactoryHttp(serviceUri);
		QueryExecution result = qef.createQueryExecution(query);
		
		return result;
	}

	@Override
	public QueryExecution createQueryExecution(Query query) {

		//create a query execution over dbpedia
		String serviceUri = "http://dbpedia.org/sparql";
		QueryExecutionFactory qef = new QueryExecutionFactoryHttp(serviceUri);

		//setup caching
		try {
			//cache for a day
			//name the db 'sparql' and use compression
			long ttl = 24l * 60l * 60l * 1000l;
			CacheBackend cacheBackend = CacheCoreH2.create("sparql", ttl, true);
			CacheFrontend cacheFrontend = new CacheFrontendImpl(cacheBackend);

			qef = new QueryExecutionFactoryCacheEx(qef, cacheFrontend);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}

		QueryExecution result = qef.createQueryExecution(query);

		return result;
	}
}

