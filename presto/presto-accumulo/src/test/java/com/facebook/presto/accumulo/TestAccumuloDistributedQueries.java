/*
 * Copyright 2016 Bloomberg L.P.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.accumulo;

import com.facebook.presto.tests.AbstractTestDistributedQueries;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;

import static com.facebook.presto.accumulo.AccumuloQueryRunner.createAccumuloQueryRunner;
import static com.facebook.presto.accumulo.AccumuloQueryRunner.dropTpchTables;

public class TestAccumuloDistributedQueries
        extends AbstractTestDistributedQueries
{
    public TestAccumuloDistributedQueries()
            throws Exception
    {
        super(createAccumuloQueryRunner(ImmutableMap.of(), true));
    }

    @AfterClass
    public void cleanup()
    {
        dropTpchTables(queryRunner, getSession());
    }
}
