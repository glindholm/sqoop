package org.apache.sqoop.mapreduce.hcat;

import static org.junit.Assert.assertEquals;

import java.sql.Types;

import org.junit.Test;

public class SqoopHCatUtilitiesTest {

  @Test
  public void testSqlTypeStrings() {
    
    int[] types = {Types.ARRAY,
        Types.BIGINT,
        Types.BINARY,
        Types.BIT,
        Types.BLOB,
        Types.BOOLEAN,
        Types.CHAR,
        Types.CLOB,
        Types.DATALINK,
        Types.DATE,
        Types.DECIMAL,
        Types.DISTINCT,
        Types.DOUBLE,
        Types.FLOAT,
        Types.INTEGER,
        Types.JAVA_OBJECT,
        Types.LONGNVARCHAR,
        Types.LONGVARBINARY,
        Types.LONGVARCHAR,
        Types.NCHAR,
        Types.NCLOB,
        Types.NULL,
        Types.NUMERIC,
        Types.NVARCHAR,
        //Types.OTHER,
        Types.REAL,
        Types.REF,
        Types.ROWID,
        Types.SMALLINT,
        Types.SQLXML,
        Types.STRUCT,
        Types.TIME,
        Types.TIMESTAMP,
        Types.TINYINT,
        Types.VARBINARY,
        Types.VARCHAR};
    
    for (int type : types) {
      String typeStr = SqoopHCatUtilities.sqlTypeString(type);
      assertEquals(typeStr, type, SqoopHCatUtilities.sqlTypeToInt(typeStr));
    }

  }
}
