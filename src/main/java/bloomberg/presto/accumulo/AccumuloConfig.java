/*
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
package bloomberg.presto.accumulo;

import javax.validation.constraints.NotNull;

import io.airlift.configuration.Config;

public class AccumuloConfig {
    private String instance;
    private String zooKeepers;
    private String username;
    private String password;

    @NotNull
    public String getInstance() {
        return this.instance;
    }

    @Config("instance")
    public AccumuloConfig setInstance(String instance) {
        this.instance = instance;
        return this;
    }

    @NotNull
    public String getZooKeepers() {
        return this.zooKeepers;
    }

    @Config("zookeepers")
    public AccumuloConfig setZooKeepers(String zooKeepers) {
        this.zooKeepers = zooKeepers;
        return this;
    }

    @NotNull
    public String getUsername() {
        return this.username;
    }

    @Config("username")
    public AccumuloConfig setUsername(String username) {
        this.username = username;
        return this;
    }

    @NotNull
    public String getPassword() {
        return this.password;
    }

    @Config("password")
    public AccumuloConfig setPassword(String password) {
        this.password = password;
        return this;
    }
}
