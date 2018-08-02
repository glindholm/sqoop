/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sqoop.mapreduce.hcat;

import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.math.BigInteger;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.junit.Test;

public class SqoopHCatImportHelperTest {

    @Test
    public void testConvertNumberTypesDecimal() throws Exception {

        boolean bigDecimalFormatString = true;

        HCatFieldSchema DECIMAL_HCAT_FIELD = new HCatFieldSchema("field1", HCatFieldSchema.Type.DECIMAL, null);

        BigDecimal bd40 = new BigDecimal("12345678901234567890.12345678901234567890");
        assertEquals("BigDecimal should not loose precision", HiveDecimal.create(bd40),
                SqoopHCatImportHelper.convertNumberTypes(bigDecimalFormatString, bd40, DECIMAL_HCAT_FIELD));

        BigInteger bi40 = new BigInteger("1234567890123456789012345678901234567890");
        assertEquals("BigInteger should not loose precision", HiveDecimal.create(bi40),
                SqoopHCatImportHelper.convertNumberTypes(bigDecimalFormatString, bi40, DECIMAL_HCAT_FIELD));

        Double float15 = 1234567890.12345d;
        assertEquals("Double should preserve at least 15 digits of precision", HiveDecimal.create(new BigDecimal(float15)),
                SqoopHCatImportHelper.convertNumberTypes(bigDecimalFormatString, float15, DECIMAL_HCAT_FIELD));

    }

}
