/*
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.redis.connection.jedis;

import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.connection.ClusterCommandExecutor.NodeResult;
import org.springframework.data.redis.connection.RedisClusterNode;
import org.springframework.data.redis.connection.RedisClusterServerCommands;
import org.springframework.data.redis.connection.RedisNode;
import org.springframework.data.redis.connection.convert.Converters;
import org.springframework.data.redis.connection.jedis.JedisClusterConnection.JedisClusterCommandCallback;
import org.springframework.data.redis.core.types.RedisClientInfo;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

/**
 * @author Mark Paluch
 * @since 2.0
 */
class JedisClusterServerCommands implements RedisClusterServerCommands {

	private final JedisClusterConnection connection;

	public JedisClusterServerCommands(JedisClusterConnection connection) {
		this.connection = connection;
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterServerCommands#bgReWriteAof(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public void bgReWriteAof(RedisClusterNode node) {

		connection.getClusterCommandExecutor().executeCommandOnSingleNode(new JedisClusterCommandCallback<String>() {

			@Override
			public String doInCluster(Jedis client) {
				return client.bgrewriteaof();
			}
		}, node);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#bgReWriteAof()
	 */
	@Override
	public void bgReWriteAof() {

		connection.getClusterCommandExecutor().executeCommandOnAllNodes(new JedisClusterCommandCallback<String>() {

			@Override
			public String doInCluster(Jedis client) {
				return client.bgrewriteaof();
			}
		});
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#bgSave()
	 */
	@Override
	public void bgSave() {

		connection.getClusterCommandExecutor().executeCommandOnAllNodes(new JedisClusterCommandCallback<String>() {

			@Override
			public String doInCluster(Jedis client) {
				return client.bgsave();
			}
		});
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterServerCommands#bgSave(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public void bgSave(RedisClusterNode node) {

		connection.getClusterCommandExecutor().executeCommandOnSingleNode(new JedisClusterCommandCallback<String>() {

			@Override
			public String doInCluster(Jedis client) {
				return client.bgsave();
			}
		}, node);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#lastSave()
	 */
	@Override
	public Long lastSave() {

		List<Long> result = new ArrayList<>(
				connection.getClusterCommandExecutor().executeCommandOnAllNodes(new JedisClusterCommandCallback<Long>() {

					@Override
					public Long doInCluster(Jedis client) {
						return client.lastsave();
					}
				}).resultsAsList());

		if (CollectionUtils.isEmpty(result)) {
			return null;
		}

		Collections.sort(result, Collections.reverseOrder());
		return result.get(0);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterServerCommands#lastSave(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public Long lastSave(RedisClusterNode node) {

		return connection.getClusterCommandExecutor().executeCommandOnSingleNode(new JedisClusterCommandCallback<Long>() {

			@Override
			public Long doInCluster(Jedis client) {
				return client.lastsave();
			}
		}, node).getValue();
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#save()
	 */
	@Override
	public void save() {

		connection.getClusterCommandExecutor().executeCommandOnAllNodes(new JedisClusterCommandCallback<String>() {

			@Override
			public String doInCluster(Jedis client) {
				return client.save();
			}
		});
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterServerCommands#save(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public void save(RedisClusterNode node) {

		connection.getClusterCommandExecutor().executeCommandOnSingleNode(new JedisClusterCommandCallback<String>() {

			@Override
			public String doInCluster(Jedis client) {
				return client.save();
			}
		}, node);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#dbSize()
	 */
	@Override
	public Long dbSize() {

		Collection<Long> dbSizes = connection.getClusterCommandExecutor()
				.executeCommandOnAllNodes(new JedisClusterCommandCallback<Long>() {

					@Override
					public Long doInCluster(Jedis client) {
						return client.dbSize();
					}
				}).resultsAsList();

		if (CollectionUtils.isEmpty(dbSizes)) {
			return 0L;
		}

		Long size = 0L;
		for (Long value : dbSizes) {
			size += value;
		}
		return size;
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterServerCommands#dbSize(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public Long dbSize(RedisClusterNode node) {

		return connection.getClusterCommandExecutor().executeCommandOnSingleNode(new JedisClusterCommandCallback<Long>() {

			@Override
			public Long doInCluster(Jedis client) {
				return client.dbSize();
			}
		}, node).getValue();
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#flushDb()
	 */
	@Override
	public void flushDb() {

		connection.getClusterCommandExecutor().executeCommandOnAllNodes(new JedisClusterCommandCallback<String>() {

			@Override
			public String doInCluster(Jedis client) {
				return client.flushDB();
			}
		});
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterServerCommands#flushDb(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public void flushDb(RedisClusterNode node) {

		connection.getClusterCommandExecutor().executeCommandOnSingleNode(new JedisClusterCommandCallback<String>() {

			@Override
			public String doInCluster(Jedis client) {
				return client.flushDB();
			}
		}, node);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#flushAll()
	 */
	@Override
	public void flushAll() {

		connection.getClusterCommandExecutor().executeCommandOnAllNodes(new JedisClusterCommandCallback<String>() {

			@Override
			public String doInCluster(Jedis client) {
				return client.flushAll();
			}
		});
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterServerCommands#flushAll(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public void flushAll(RedisClusterNode node) {

		connection.getClusterCommandExecutor().executeCommandOnSingleNode(new JedisClusterCommandCallback<String>() {

			@Override
			public String doInCluster(Jedis client) {
				return client.flushAll();
			}
		}, node);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#info()
	 */
	@Override
	public Properties info() {

		Properties infos = new Properties();

		List<NodeResult<Properties>> nodeResults = connection.getClusterCommandExecutor()
				.executeCommandOnAllNodes(new JedisClusterCommandCallback<Properties>() {

					@Override
					public Properties doInCluster(Jedis client) {
						return JedisConverters.toProperties(client.info());
					}
				}).getResults();

		for (NodeResult<Properties> nodeProperties : nodeResults) {
			for (Entry<Object, Object> entry : nodeProperties.getValue().entrySet()) {
				infos.put(nodeProperties.getNode().asString() + "." + entry.getKey(), entry.getValue());
			}
		}

		return infos;
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterServerCommands#info(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public Properties info(RedisClusterNode node) {

		return JedisConverters.toProperties(
				connection.getClusterCommandExecutor().executeCommandOnSingleNode(new JedisClusterCommandCallback<String>() {

					@Override
					public String doInCluster(Jedis client) {
						return client.info();
					}
				}, node).getValue());
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#info(java.lang.String)
	 */
	@Override
	public Properties info(final String section) {

		Properties infos = new Properties();

		List<NodeResult<Properties>> nodeResults = connection.getClusterCommandExecutor()
				.executeCommandOnAllNodes(new JedisClusterCommandCallback<Properties>() {

					@Override
					public Properties doInCluster(Jedis client) {
						return JedisConverters.toProperties(client.info(section));
					}
				}).getResults();

		for (NodeResult<Properties> nodeProperties : nodeResults) {
			for (Entry<Object, Object> entry : nodeProperties.getValue().entrySet()) {
				infos.put(nodeProperties.getNode().asString() + "." + entry.getKey(), entry.getValue());
			}
		}

		return infos;
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterServerCommands#info(org.springframework.data.redis.connection.RedisClusterNode, java.lang.String)
	 */
	@Override
	public Properties info(RedisClusterNode node, final String section) {

		return JedisConverters.toProperties(
				connection.getClusterCommandExecutor().executeCommandOnSingleNode(new JedisClusterCommandCallback<String>() {

					@Override
					public String doInCluster(Jedis client) {
						return client.info(section);
					}
				}, node).getValue());
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#shutdown()
	 */
	@Override
	public void shutdown() {

		connection.getClusterCommandExecutor().executeCommandOnAllNodes(new JedisClusterCommandCallback<String>() {

			@Override
			public String doInCluster(Jedis client) {
				return client.shutdown();
			}
		});
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterServerCommands#shutdown(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public void shutdown(RedisClusterNode node) {

		connection.getClusterCommandExecutor().executeCommandOnSingleNode(new JedisClusterCommandCallback<String>() {

			@Override
			public String doInCluster(Jedis client) {
				return client.shutdown();
			}
		}, node);

	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#shutdown(org.springframework.data.redis.connection.RedisServerCommands.ShutdownOption)
	 */
	@Override
	public void shutdown(ShutdownOption option) {

		if (option == null) {
			shutdown();
			return;
		}

		throw new IllegalArgumentException("Shutdown with options is not supported for jedis.");
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#getConfig(java.lang.String)
	 */
	@Override
	public List<String> getConfig(final String pattern) {

		List<NodeResult<List<String>>> mapResult = connection.getClusterCommandExecutor()
				.executeCommandOnAllNodes(new JedisClusterCommandCallback<List<String>>() {

					@Override
					public List<String> doInCluster(Jedis client) {
						return client.configGet(pattern);
					}
				}).getResults();

		List<String> result = new ArrayList<>();
		for (NodeResult<List<String>> entry : mapResult) {

			String prefix = entry.getNode().asString();
			int i = 0;
			for (String value : entry.getValue()) {
				result.add((i++ % 2 == 0 ? (prefix + ".") : "") + value);
			}
		}

		return result;
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterServerCommands#getConfig(org.springframework.data.redis.connection.RedisClusterNode, java.lang.String)
	 */
	@Override
	public List<String> getConfig(RedisClusterNode node, final String pattern) {

		return connection.getClusterCommandExecutor()
				.executeCommandOnSingleNode(new JedisClusterCommandCallback<List<String>>() {

					@Override
					public List<String> doInCluster(Jedis client) {
						return client.configGet(pattern);
					}
				}, node).getValue();
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#setConfig(java.lang.String, java.lang.String)
	 */
	@Override
	public void setConfig(final String param, final String value) {

		connection.getClusterCommandExecutor().executeCommandOnAllNodes(new JedisClusterCommandCallback<String>() {

			@Override
			public String doInCluster(Jedis client) {
				return client.configSet(param, value);
			}
		});
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterServerCommands#setConfig(org.springframework.data.redis.connection.RedisClusterNode, java.lang.String, java.lang.String)
	 */
	@Override
	public void setConfig(RedisClusterNode node, final String param, final String value) {

		connection.getClusterCommandExecutor().executeCommandOnSingleNode(new JedisClusterCommandCallback<String>() {

			@Override
			public String doInCluster(Jedis client) {
				return client.configSet(param, value);
			}
		}, node);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#resetConfigStats()
	 */
	@Override
	public void resetConfigStats() {

		connection.getClusterCommandExecutor().executeCommandOnAllNodes(new JedisClusterCommandCallback<String>() {

			@Override
			public String doInCluster(Jedis client) {
				return client.configResetStat();
			}
		});
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterServerCommands#resetConfigStats(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public void resetConfigStats(RedisClusterNode node) {

		connection.getClusterCommandExecutor().executeCommandOnSingleNode(new JedisClusterCommandCallback<String>() {

			@Override
			public String doInCluster(Jedis client) {
				return client.configResetStat();
			}
		}, node);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#time()
	 */
	@Override
	public Long time() {

		return convertListOfStringToTime(connection.getClusterCommandExecutor()
				.executeCommandOnArbitraryNode(new JedisClusterCommandCallback<List<String>>() {

					@Override
					public List<String> doInCluster(Jedis client) {
						return client.time();
					}
				}).getValue());
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterServerCommands#time(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public Long time(RedisClusterNode node) {

		return convertListOfStringToTime(connection.getClusterCommandExecutor()
				.executeCommandOnSingleNode(new JedisClusterCommandCallback<List<String>>() {

					@Override
					public List<String> doInCluster(Jedis client) {
						return client.time();
					}
				}, node).getValue());
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#killClient(java.lang.String, int)
	 */
	@Override
	public void killClient(String host, int port) {

		final String hostAndPort = String.format("%s:%s", host, port);

		connection.getClusterCommandExecutor().executeCommandOnAllNodes(new JedisClusterCommandCallback<String>() {

			@Override
			public String doInCluster(Jedis client) {
				return client.clientKill(hostAndPort);
			}
		});
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#setClientName(byte[])
	 */
	@Override
	public void setClientName(byte[] name) {
		throw new InvalidDataAccessApiUsageException("CLIENT SETNAME is not supported in cluster environment.");
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#getClientName()
	 */
	@Override
	public String getClientName() {
		throw new InvalidDataAccessApiUsageException("CLIENT GETNAME is not supported in cluster environment.");
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#getClientList()
	 */
	@Override
	public List<RedisClientInfo> getClientList() {

		Collection<String> map = connection.getClusterCommandExecutor()
				.executeCommandOnAllNodes(new JedisClusterCommandCallback<String>() {

					@Override
					public String doInCluster(Jedis client) {
						return client.clientList();
					}
				}).resultsAsList();

		ArrayList<RedisClientInfo> result = new ArrayList<>();
		for (String infos : map) {
			result.addAll(JedisConverters.toListOfRedisClientInformation(infos));
		}
		return result;
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterServerCommands#getClientList(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public List<RedisClientInfo> getClientList(RedisClusterNode node) {

		return JedisConverters.toListOfRedisClientInformation(
				connection.getClusterCommandExecutor().executeCommandOnSingleNode(new JedisClusterCommandCallback<String>() {

					@Override
					public String doInCluster(Jedis client) {
						return client.clientList();
					}
				}, node).getValue());
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#slaveOf(java.lang.String, int)
	 */
	@Override
	public void slaveOf(String host, int port) {
		throw new InvalidDataAccessApiUsageException(
				"SlaveOf is not supported in cluster environment. Please use CLUSTER REPLICATE.");
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#slaveOfNoOne()
	 */
	@Override
	public void slaveOfNoOne() {
		throw new InvalidDataAccessApiUsageException(
				"SlaveOf is not supported in cluster environment. Please use CLUSTER REPLICATE.");
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#migrate(byte[], org.springframework.data.redis.connection.RedisNode, int, org.springframework.data.redis.connection.RedisServerCommands.MigrateOption)
	 */
	@Override
	public void migrate(byte[] key, RedisNode target, int dbIndex, MigrateOption option) {
		migrate(key, target, dbIndex, option, Long.MAX_VALUE);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisServerCommands#migrate(byte[], org.springframework.data.redis.connection.RedisNode, int, org.springframework.data.redis.connection.RedisServerCommands.MigrateOption, long)
	 */
	@Override
	public void migrate(final byte[] key, final RedisNode target, final int dbIndex, final MigrateOption option,
			final long timeout) {

		final int timeoutToUse = timeout <= Integer.MAX_VALUE ? (int) timeout : Integer.MAX_VALUE;

		RedisClusterNode node = connection.getTopologyProvider().getTopology().lookup(target.getHost(), target.getPort());

		connection.getClusterCommandExecutor().executeCommandOnSingleNode(new JedisClusterCommandCallback<String>() {

			@Override
			public String doInCluster(Jedis client) {
				return client.migrate(JedisConverters.toBytes(target.getHost()), target.getPort(), key, dbIndex, timeoutToUse);
			}
		}, node);
	}

	private Long convertListOfStringToTime(List<String> serverTimeInformation) {

		Assert.notEmpty(serverTimeInformation, "Received invalid result from server. Expected 2 items in collection.");
		Assert.isTrue(serverTimeInformation.size() == 2,
				"Received invalid number of arguments from redis server. Expected 2 received " + serverTimeInformation.size());

		return Converters.toTimeMillis(serverTimeInformation.get(0), serverTimeInformation.get(1));
	}
}
