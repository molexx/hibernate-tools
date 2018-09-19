package org.hibernate.cfg.reveng.dialect;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * MetaDataDialect for Postgres
 *
 * Attempts to determine when the "sequence" strategy should
 * be used for table primary keys
 * 
 * Taken from: https://hibernate.atlassian.net/browse/HBX-1165
 * 
 *
 * @author Adam Retter <adam.retter@googlemail.com> / www.adamretter.org.uk
 * @serial 201006281233
 */
public class PostgresMetaDataDialect extends JDBCMetaDataDialect {

	@Override
	public Iterator<Map<String, Object>> getSuggestedPrimaryKeyStrategyName(String catalog, String schema, String table) {

		String sql = null;
		try {
			catalog = caseForSearch( catalog );
			schema = caseForSearch( schema );
			table = caseForSearch( table );

			log.debug("getSuggestedPrimaryKeyStrategyName(" + catalog + "." + schema + "." + table + ")");

			sql = "select nsp.nspname, " +
					"cls.relname, " +
					"att.attname, " +
					"att.attidentity, " +
					"con.conname, " +
					"pg_get_serial_sequence(nsp.nspname || '.' || cls.relname, att.attname) seqname " +
					"from pg_catalog.pg_namespace nsp, pg_catalog.pg_class cls, pg_catalog.pg_constraint con, pg_catalog.pg_attribute att " +
					"where " +
					"cls.relnamespace = nsp.oid and " +
					"con.conrelid = cls.oid and " +
					"att.attrelid = cls.oid and " +
					"att.attnum = ANY(con.conkey) and " +
					"con.contype = 'p'";

			if(schema != null || table != null) {
				sql += " and ";

				if(schema != null) {
					sql += "nsp.nspname = '" + schema + "'";
				}

				if(schema != null && table != null) {
					sql += " and ";
				}

				if(table != null) {
					sql += "cls.relname= '" + table + "'";
				}
			}

			System.out.println("PostgresMetaDataDialect.getSuggestedPrimaryKeyStrategyName(): querying for sequence with sql: " + sql);
			
			PreparedStatement statement = getConnection().prepareStatement(sql);
			
			final String sc = schema;
			return new ResultSetIterator(statement.executeQuery(), getSQLExceptionConverter()) {
				Map<String, Object> element = new HashMap<String, Object>();
				protected Map<String, Object>  convertRow(ResultSet rs) throws SQLException {
					element.clear();
					element.put("TABLE_NAME", rs.getString("relname"));
					element.put("TABLE_SCHEM", rs.getString("nspname"));
					element.put("TABLE_CAT", null);
					
					StringBuffer row = new StringBuffer();
					for (int i = 1; i < 6; i++) {
						row.append(i + ": " + rs.getObject(i) + ", ");
					}
					
					System.out.println("PostgresMetaDataDialect.getSuggestedPrimaryKeyStrategyName(): row: " + row);
					
					String seqName = rs.getString("seqname");
					if(seqName != null && seqName.length() > 0) {
						/*
						This breaks because JPA/Hibernate defaults to a sequence interval of 50, but if you've already created the sequence in the database its interval is probably not 50
						So the proper fix is to query for the sequence's interval and set it as a param on the annotation.
						For now, just use IDENTITY. This affects performance when batching multiple inserts. 
						
						String identity = rs.getString("attidentity");
						System.out.println("PostgresMetaDataDialect.getSuggestedPrimaryKeyStrategyName(): sequence name found: '" + seqName + "', identity: " + identity);
						if (identity.equals("a")) {  //ALWAYS - value cannot be set by Java 
							element.put("HIBERNATE_STRATEGY", "identity");
						} else {
							element.put("HIBERNATE_STRATEGY", "sequence");
						}*/
						//TODO get the sequence's interval and set as an attribute param
						
						//for now always return "identity" if there's a sequence
						element.put("HIBERNATE_STRATEGY", "identity");
					} else {
						//no sequence
						element.put("HIBERNATE_STRATEGY", null);
					}
					
					//TODO how to also set the sequence name?
					
					return element;
				}
				protected Throwable handleSQLException(SQLException e) {
					// schemaRs and catalogRs are only used for error reporting if
					// we get an exception
					throw getSQLExceptionConverter().convert(e, "Could not get list of suggested identity strategies from database. Probably a JDBC driver problem. ", null);
				}
			};
		} catch (SQLException e) {
			throw getSQLExceptionConverter().convert(e, "Could not get list of suggested identity strategies from database. Probably a JDBC driver problem. ", sql);
		}
	}
}