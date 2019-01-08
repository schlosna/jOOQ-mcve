/*
 * This work is dual-licensed
 * - under the Apache Software License 2.0 (the "ASL")
 * - under the jOOQ License and Maintenance Agreement (the "jOOQ License")
 * =============================================================================
 * You may choose which license applies to you:
 *
 * - If you're using this work with Open Source databases, you may choose
 *   either ASL or jOOQ License.
 * - If you're using this work with at least one commercial database, you must
 *   choose jOOQ License
 *
 * For more information, please visit http://www.jooq.org/licenses
 *
 * Apache Software License 2.0:
 * -----------------------------------------------------------------------------
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * jOOQ License and Maintenance Agreement:
 * -----------------------------------------------------------------------------
 * Data Geekery grants the Customer the non-exclusive, timely limited and
 * non-transferable license to install and use the Software under the terms of
 * the jOOQ License and Maintenance Agreement.
 *
 * This library is distributed with a LIMITED WARRANTY. See the jOOQ License
 * and Maintenance Agreement for more details: http://www.jooq.org/licensing
 */
package org.jooq.mcve.test;

import static org.jooq.mcve.Tables.TEST;
import static org.junit.Assert.assertEquals;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class MCVETest {

    // Oracle IN limit is max 65535 inclusive
    private static final int ORACLE_IN_LIMIT = 65_535;
    private static final int EXPECTED_ROWS = ORACLE_IN_LIMIT + 64;
    private static final List<Integer> IDS = IntStream.range(0, EXPECTED_ROWS).boxed()
            .collect(Collectors.toList());

    private static boolean USE_ORACLE = true;
    private static Connection connection;
    private DSLContext ctx;

    @BeforeClass
    public static void beforeClass() throws Exception {
        if (USE_ORACLE) {
            // test example using Oracle XE https://hub.docker.com/r/sath89/oracle-12c/
            String url = "jdbc:oracle:thin:@localhost:1521:xe";
            connection = DriverManager.getConnection(url, "system", "oracle");
        } else {
            connection = DriverManager.getConnection("jdbc:h2:~/mcve", "sa", "");
        }

        setupTable(DSL.using(connection));
    }

    private static void setupTable(DSLContext ctx) {
        int count = ctx.select(DSL.count())
                .from(TEST)
                .fetchOne(0, int.class);
        if (count != EXPECTED_ROWS) {
            ctx.truncate(TEST).cascade().execute();
            batchInsert(ctx, EXPECTED_ROWS);
        }
    }

    @AfterClass
    public static void afterClass() throws Exception {
        connection.close();
        connection = null;
    }

    @Before
    public void setup() {
        ctx = DSL.using(connection);
    }

    @After
    public void after() {
        ctx = null;
    }

    @Test
    public void whereIn() {
        // the following WHERE IN fails on Oracle with ORA-01745: invalid host/bind variable name
        // when more than 65535 ids are provided
        int count = ctx.select(DSL.count()).from(TEST)
                .where(TEST.ID.in(IDS))
                .fetchOne(0, int.class);
        System.out.println(count);
        assertEquals(EXPECTED_ROWS, count);
    }

    private static void batchInsert(DSLContext ctx, int expectedRows) {
        int[] execute = ctx.batch(
                IntStream.range(0, expectedRows).mapToObj(i ->
                        ctx.insertInto(TEST)
                                .columns(TEST.ID, TEST.VALUE)
                                .values(i, i))
                        .collect(Collectors.toList())
        ).execute();
        int inserted = IntStream.of(execute).sum();
        assertEquals(expectedRows, inserted);
    }
}
