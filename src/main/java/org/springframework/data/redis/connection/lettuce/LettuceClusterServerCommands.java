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
package org.springframework.data.redis.connection.lettuce;

import io.lettuce.core.cluster.api.sync.RedisClusterCommands;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.connection.ClusterCommandExecutor.NodeResult;
import org.springframework.data.redis.connection.RedisClusterNode;
import org.springframework.data.redis.connection.RedisClusterServerCommands;
import org.springframework.data.redis.connection.convert.Converters;
import org.springframework.data.redis.connection.lettuce.LettuceClusterConnection.LettuceClusterCommandCallback;
import org.springframework.data.redis.core.types.RedisClientInfo;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

/**
 * @author Mark Paluch
 * @since 2.0
 */
public class LettuceClusterServerCommands extends LettuceServerCommands implements RedisClusterServerCommands {

	private final LettuceClusterConnection connection;

	public LettuceClusterServerCommands(LettuceClusterConnection connection) {

		super(connection);
		this.connection = connection;
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterServerCommands#bgReWriteAof(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public void bgReWriteAof(RedisClusterNode node) {

		connection.getClusterCommandExecutor().executeCommandOnSingleNode(new LettuceClusterCommandCallback<String>() {

			@Override
			public String doInCluster(RedisClusterCommands<byte[], byte[]> client) {
				return client.bgrewriteaof();
			}
		}, node);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterServerCommands#bgSave(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public void bgSave(RedisClusterNode node) {

		connection.getClusterCommandExecutor().executeCommandOnSingleNode(new LettuceClusterCommandCallback<String>() {

			@Override
			public String doInCluster(RedisClusterCommands<byte[], byte[]> client) {
				return client.bgsave();
			}
		}, node);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterServerCommands#lastSave(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public Long lastSave(RedisClusterNode node) {

		return connection.getClusterCommandExecutor().executeCommandOnSingleNode(new LettuceClusterCommandCallback<Long>() {

			@Override
			public Long doInCluster(RedisClusterCommands<byte[], byte[]> client) {
				return client.lastsave().getTime();
			}
		}, node).getValue();
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterServerCommands#save(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public void save(RedisClusterNode node) {

		connection.getClusterCommandExecutor().executeCommandOnSingleNode(new LettuceClusterCommandCallback<String>() {

			@Override
			public String doInCluster(RedisClusterCommands<byte[], byte[]> client) {
				return client.save();
			}
		}, node);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceServerCommands#dbSize()
	 */
	@Override
	public Long dbSize() {

		Collection<Long> dbSizes = connection.getClusterCommandExecutor()
				.executeCommandOnAllNodes(new LettuceClusterCommandCallback<Long>() {

					@Override
					public Long doInCluster(RedisClusterCommands<byte[], byte[]> client) {
						return client.dbsize();
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

		return connection.getClusterCommandExecutor().executeCommandOnSingleNode(new LettuceClusterCommandCallback<Long>() {

			@Override
			public Long doInCluster(RedisClusterCommands<byte[], byte[]> client) {
				return client.dbsize();
			}
		}, node).getValue();
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceServerCommands#flushDb()
	 */
	@Override
	public void flushDb() {

		connection.getClusterCommandExecutor().executeCommandOnAllNodes(new LettuceClusterCommandCallback<String>() {

			@Override
			public String doInCluster(RedisClusterCommands<byte[], byte[]> client) {
				return client.flushdb();
			}
		});
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterServerCommands#flushDb(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public void flushDb(RedisClusterNode node) {

		connection.getClusterCommandExecutor().executeCommandOnSingleNode(new LettuceClusterCommandCallback<String>() {

			@Override
			public String doInCluster(RedisClusterCommands<byte[], byte[]> client) {
				return client.flushdb();
			}
		}, node);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceServerCommands#flushAll()
	 */
	@Override
	public void flushAll() {

		connection.getClusterCommandExecutor().executeCommandOnAllNodes(new LettuceClusterCommandCallback<String>() {

			@Override
			public String doInCluster(RedisClusterCommands<byte[], byte[]> client) {
				return client.flushall();
			}
		});
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterServerCommands#flushAll(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public void flushAll(RedisClusterNode node) {

		connection.getClusterCommandExecutor().executeCommandOnSingleNode(new LettuceClusterCommandCallback<String>() {

			@Override
			public String doInCluster(RedisClusterCommands<byte[], byte[]> client) {
				return client.flushall();
			}
		}, node);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterServerCommands#info(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public Properties info(RedisClusterNode node) {

		return LettuceConverters.toProperties(
				connection.getClusterCommandExecutor().executeCommandOnSingleNode(new LettuceClusterCommandCallback<String>() {

					@Override
					public String doInCluster(RedisClusterCommands<byte[], byte[]> client) {
						return client.info();
					}
				}, node).getValue());
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceServerCommands#info()
	 */
	@Override
	public Properties info() {

		Properties infos = new Properties();

		List<NodeResult<Properties>> nodeResults = connection.getClusterCommandExecutor()
				.executeCommandOnAllNodes(new LettuceClusterCommandCallback<Properties>() {

					@Override
					public Properties doInCluster(RedisClusterCommands<byte[], byte[]> client) {
						return LettuceConverters.toProperties(client.info());
					}
				}).getResults();

		for (NodeResult<Properties> nodePorperties : nodeResults) {
			for (Entry<Object, Object> entry : nodePorperties.getValue().entrySet()) {
				infos.put(nodePorperties.getNode().asString() + "." + entry.getKey(), entry.getValue());
			}
		}

		return infos;
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceServerCommands#info(java.lang.String)
	 */
	@Override
	public Properties info(final String section) {

		Properties infos = new Properties();
		List<NodeResult<Properties>> nodeResults = connection.getClusterCommandExecutor()
				.executeCommandOnAllNodes(new LettuceClusterCommandCallback<Properties>() {

					@Override
					public Properties doInCluster(RedisClusterCommands<byte[], byte[]> client) {
						return LettuceConverters.toProperties(client.info(section));
					}
				}).getResults();

		for (NodeResult<Properties> nodePorperties : nodeResults) {
			for (Entry<Object, Object> entry : nodePorperties.getValue().entrySet()) {
				infos.put(nodePorperties.getNode().asString() + "." + entry.getKey(), entry.getValue());
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

		return LettuceConverters.toProperties(
				connection.getClusterCommandExecutor().executeCommandOnSingleNode(new LettuceClusterCommandCallback<String>() {

					@Override
					public String doInCluster(RedisClusterCommands<byte[], byte[]> client) {
						return client.info(section);
					}
				}, node).getValue());
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterServerCommands#shutdown(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public void shutdown(RedisClusterNode node) {

		connection.getClusterCommandExecutor().executeCommandOnSingleNode(new LettuceClusterCommandCallback<Void>() {

			@Override
			public Void doInCluster(RedisClusterCommands<byte[], byte[]> client) {
				client.shutdown(true);
				return null;
			}
		}, node);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceServerCommands#getConfig(java.lang.String)
	 */
	@Override
	public List<String> getConfig(final String pattern) {

		List<NodeResult<List<String>>> mapResult = connection.getClusterCommandExecutor()
				.executeCommandOnAllNodes(new LettuceClusterCommandCallback<List<String>>() {

					@Override
					public List<String> doInCluster(RedisClusterCommands<byte[], byte[]> client) {
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
				.executeCommandOnSingleNode(new LettuceClusterCommandCallback<List<String>>() {

					@Override
					public List<String> doInCluster(RedisClusterCommands<byte[], byte[]> client) {
						return client.configGet(pattern);
					}
				}, node).getValue();
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceServerCommands#setConfig(java.lang.String, java.lang.String)
	 */
	@Override
	public void setConfig(final String param, final String value) {

		connection.getClusterCommandExecutor().executeCommandOnAllNodes(new LettuceClusterCommandCallback<String>() {

			@Override
			public String doInCluster(RedisClusterCommands<byte[], byte[]> client) {
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

		connection.getClusterCommandExecutor().executeCommandOnSingleNode(new LettuceClusterCommandCallback<String>() {

			@Override
			public String doInCluster(RedisClusterCommands<byte[], byte[]> client) {
				return client.configSet(param, value);
			}
		}, node);

	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceServerCommands#resetConfigStats()
	 */
	@Override
	public void resetConfigStats() {

		connection.getClusterCommandExecutor().executeCommandOnAllNodes(new LettuceClusterCommandCallback<String>() {

			@Override
			public String doInCluster(RedisClusterCommands<byte[], byte[]> client) {
				return client.configResetstat();
			}
		});
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterServerCommands#resetConfigStats(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public void resetConfigStats(RedisClusterNode node) {

		connection.getClusterCommandExecutor().executeCommandOnSingleNode(new LettuceClusterCommandCallback<String>() {

			@Override
			public String doInCluster(RedisClusterCommands<byte[], byte[]> client) {
				return client.configResetstat();
			}
		}, node);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceServerCommands#time()
	 */
	@Override
	public Long time() {

		return convertListOfStringToTime(connection.getClusterCommandExecutor()
				.executeCommandOnArbitraryNode(new LettuceClusterCommandCallback<List<byte[]>>() {

					@Override
					public List<byte[]> doInCluster(RedisClusterCommands<byte[], byte[]> client) {
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
				.executeCommandOnSingleNode(new LettuceClusterCommandCallback<List<byte[]>>() {

					@Override
					public List<byte[]> doInCluster(RedisClusterCommands<byte[], byte[]> client) {
						return client.time();
					}
				}, node).getValue());
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceServerCommands#getClientList()
	 */
	@Override
	public List<RedisClientInfo> getClientList() {

		List<String> map = connection.getClusterCommandExecutor()
				.executeCommandOnAllNodes(new LettuceClusterCommandCallback<String>() {

					@Override
					public String doInCluster(RedisClusterCommands<byte[], byte[]> client) {
						return client.clientList();
					}
				}).resultsAsList();

		ArrayList<RedisClientInfo> result = new ArrayList<>();
		for (String infos : map) {
			result.addAll(LettuceConverters.toListOfRedisClientInformation(infos));
		}
		return result;
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisClusterServerCommands#getClientList(org.springframework.data.redis.connection.RedisClusterNode)
	 */
	@Override
	public List<RedisClientInfo> getClientList(RedisClusterNode node) {

		return LettuceConverters.toListOfRedisClientInformation(
				connection.getClusterCommandExecutor().executeCommandOnSingleNode(new LettuceClusterCommandCallback<String>() {

					@Override
					public String doInCluster(RedisClusterCommands<byte[], byte[]> client) {
						return client.clientList();
					}
				}, node).getValue());
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceServerCommands#slaveOf(java.lang.String, int)
	 */
	@Override
	public void slaveOf(String host, int port) {
		throw new InvalidDataAccessApiUsageException(
				"SlaveOf is not supported in cluster environment. Please use CLUSTER REPLICATE.");
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceServerCommands#slaveOfNoOne()
	 */
	@Override
	public void slaveOfNoOne() {
		throw new InvalidDataAccessApiUsageException(
				"SlaveOf is not supported in cluster environment. Please use CLUSTER REPLICATE.");
	}

	private static Long convertListOfStringToTime(List<byte[]> serverTimeInformation) {

		Assert.notEmpty(serverTimeInformation, "Received invalid result from server. Expected 2 items in collection.");
		Assert.isTrue(serverTimeInformation.size() == 2,
				"Received invalid number of arguments from redis server. Expected 2 received " + serverTimeInformation.size());

		return Converters.toTimeMillis(LettuceConverters.toString(serverTimeInformation.get(0)),
				LettuceConverters.toString(serverTimeInformation.get(1)));
	}
}
