package org.aksw.jena_sparql_api.utils;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

/**
 * This class does not really belong to the SPARQL api,
 * but still it is useful for the server component.
 *
 * Code originated from a post on stackoverflow (forgot to note the link)
 * 
 * @author Claus Stadler
 *
 */
public class UriUtils {

	public static Multimap<String, String> parseQueryString(String queryString) {
		try {
			return parseQueryStringEx(queryString);
		} catch(Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	public static Multimap<String, String> parseQueryStringEx(String queryString)
			throws UnsupportedEncodingException
	{
        Multimap<String, String> result = ArrayListMultimap.create();
	    
        if(queryString == null) {
        	return result;
        }
        
        for (String param : queryString.split("&")) {
	        String pair[] = param.split("=");
	        String key = URLDecoder.decode(pair[0], "UTF-8");
	        String value = "";
	        if (pair.length > 1) {
	            value = URLDecoder.decode(pair[1], "UTF-8");
	        }
	        result.put(new String(key), new String(value));
	    }
	    
        return result;	
	}

}
