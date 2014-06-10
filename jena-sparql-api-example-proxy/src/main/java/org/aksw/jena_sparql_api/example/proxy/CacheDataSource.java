package org.aksw.jena_sparql_api.example.proxy;

import org.h2.jdbcx.JdbcDataSource;

public class CacheDataSource extends JdbcDataSource {

	public CacheDataSource() {
		// TODO Auto-generated constructor stub
        this.setURL("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1");
        this.setUser("sa");
        this.setPassword("sa");
	}

//	@Override
//	public boolean isWrapperFor(Class<?> arg0) throws SQLException {
//		// TODO Auto-generated method stub
//		return false;
//	}
//
//	@Override
//	public <T> T unwrap(Class<T> arg0) throws SQLException {
//		// TODO Auto-generated method stub
//		return null;
//	}
}
