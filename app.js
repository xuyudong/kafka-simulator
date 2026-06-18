(function () {
  "use strict";

  const STORAGE_KEY = "kafka-simulator-state-v1";
  const TYPE_LABELS = {
    producer: "Producer",
    topic: "Topic",
    group: "Consumer Group",
    consumer: "Consumer",
    broker: "Broker/集群"
  };
  const TYPE_INITIALS = {
    producer: "P",
    topic: "T",
    group: "G",
    consumer: "C",
    broker: "B"
  };
  const STRATEGY_LABELS = {
    "key-hash": "按 Key 哈希",
    "round-robin": "轮询 Partition",
    explicit: "指定 Partition"
  };
  const EVENT_TYPE_LABELS = {
    create: "创建",
    warn: "提醒",
    connect: "连接",
    delete: "删除",
    topic: "Topic",
    produce: "生产",
    rebalance: "再均衡",
    consume: "消费",
    commit: "提交",
    seek: "定位",
    save: "保存",
    load: "加载",
    reset: "重置",
    hint: "提示",
    edit: "编辑"
  };
  const DEFAULT_PRODUCER_DRAFT = {
    topicId: null,
    key: "客户-42",
    value: "订单已创建",
    headers: "来源=网页",
    explicitPartition: 0,
    autoPoll: true
  };
  const FLOW_DURATIONS = {
    produce: 760,
    consume: 980,
    commit: 720,
    rebalance: 720
  };
  const CANVAS_ZOOM_MIN = 0.6;
  const CANVAS_ZOOM_MAX = 2.4;
  const CANVAS_ZOOM_STEP = 0.1;
  const NODE_BASE_WIDTH = 190;
  const NODE_BASE_HEIGHT = 92;

  const scenarioFactories = {
    basic: basicScenario,
    keyed: keyedOrderingScenario,
    group: multiConsumerScenario,
    independent: independentGroupsScenario,
    lag: lagScenario,
    replay: replayScenario
  };

  let state = createEmptyState();
  let dragState = null;
  let connectState = null;
  let lastModalMode = "export";
  let flowQueue = Promise.resolve();
  let canvasZoom = 1;

  function createEmptyState() {
    return {
      version: 1,
      nextId: 1,
      nextRecordSeq: 1,
      selectedId: null,
      nodes: [],
      connections: [],
      producers: {},
      topics: {},
      groups: {},
      consumers: {},
      brokers: {},
      events: []
    };
  }

  function createPartition(partitionId) {
    return {
      id: partitionId,
      records: [],
      nextOffset: 0
    };
  }

  function allocateId(draft, type) {
    const id = `${type}-${draft.nextId}`;
    draft.nextId += 1;
    return id;
  }

  function createNode(draft, type, x, y, name) {
    const id = allocateId(draft, type);
    const nodeName = name || `${TYPE_LABELS[type]} ${id.split("-")[1]}`;
    draft.nodes.push({ id, type, x, y });

    if (type === "producer") {
      draft.producers[id] = {
        id,
        name: nodeName,
        strategy: "key-hash",
        roundRobinIndex: 0,
        lastSend: null,
        draftMessage: { ...DEFAULT_PRODUCER_DRAFT }
      };
    }

    if (type === "topic") {
      draft.topics[id] = {
        id,
        name: nodeName,
        partitionCount: 1,
        partitions: [createPartition(0)],
        retentionMode: "none",
        replicationPlaceholder: 1,
        lastRecordId: null
      };
    }

    if (type === "group") {
      draft.groups[id] = {
        id,
        name: nodeName,
        subscribedTopicIds: [],
        consumerIds: [],
        assignments: {},
        positions: {},
        committed: {}
      };
    }

    if (type === "consumer") {
      draft.consumers[id] = {
        id,
        name: nodeName,
        groupId: null,
        lastRecord: null
      };
    }

    if (type === "broker") {
      draft.brokers[id] = {
        id,
        name: nodeName
      };
    }

    return id;
  }

  function addNode(type, x, y) {
    const fallbackX = 80 + (state.nodes.length % 4) * 210;
    const fallbackY = 70 + Math.floor(state.nodes.length / 4) * 140;
    const id = createNode(state, type, x || fallbackX, y || fallbackY);
    state.selectedId = id;
    logEvent(state, "create", `${TYPE_LABELS[type]}已创建。`);
    render();
  }

  function getNode(draft, id) {
    return draft.nodes.find((node) => node.id === id) || null;
  }

  function getEntity(draft, id) {
    const node = getNode(draft, id);
    if (!node) return null;
    if (node.type === "producer") return draft.producers[id];
    if (node.type === "topic") return draft.topics[id];
    if (node.type === "group") return draft.groups[id];
    if (node.type === "consumer") return draft.consumers[id];
    if (node.type === "broker") return draft.brokers[id];
    return null;
  }

  function getEntityName(draft, id) {
    const entity = getEntity(draft, id);
    return entity ? entity.name : "未知";
  }

  function setEntityName(draft, id, name) {
    const entity = getEntity(draft, id);
    if (!entity) return;
    entity.name = name.trim() || entity.name;
  }

  function edgeExists(draft, source, target) {
    return draft.connections.some((edge) => edge.source === source && edge.target === target);
  }

  function validateConnection(draft, source, target) {
    if (!source || !target || source === target) return false;
    const sourceNode = getNode(draft, source);
    const targetNode = getNode(draft, target);
    if (!sourceNode || !targetNode) return false;
    if (sourceNode.type === "producer" && targetNode.type === "topic") return true;
    if (sourceNode.type === "topic" && targetNode.type === "group") return true;
    if (sourceNode.type === "group" && targetNode.type === "consumer") return true;
    return false;
  }

  function connectionLabel(draft, connection) {
    const sourceType = getNode(draft, connection.source)?.type;
    const targetType = getNode(draft, connection.target)?.type;
    if (sourceType === "producer" && targetType === "topic") return "生产";
    if (sourceType === "topic" && targetType === "group") return "订阅";
    if (sourceType === "group" && targetType === "consumer") return "成员";
    return "连接";
  }

  function connectNodes(draft, source, target, shouldLog) {
    if (!validateConnection(draft, source, target)) {
      if (shouldLog) logEvent(draft, "warn", "连接不符合 Kafka 消息中间件模型。请按 Producer -> Topic -> Consumer Group -> Consumer 连接。");
      return false;
    }

    if (edgeExists(draft, source, target)) {
      if (shouldLog) logEvent(draft, "warn", "连接已存在。");
      return false;
    }

    const connection = {
      id: `edge-${draft.nextId}`,
      source,
      target
    };
    draft.nextId += 1;
    draft.connections.push(connection);

    const sourceNode = getNode(draft, source);
    const targetNode = getNode(draft, target);
    if (sourceNode.type === "topic" && targetNode.type === "group") {
      const group = draft.groups[target];
      if (!group.subscribedTopicIds.includes(source)) group.subscribedTopicIds.push(source);
      rebalanceGroup(draft, target, shouldLog);
    }

    if (sourceNode.type === "group" && targetNode.type === "consumer") {
      const group = draft.groups[source];
      const consumer = draft.consumers[target];
      if (consumer.groupId && consumer.groupId !== source) {
        removeConsumerFromGroup(draft, consumer.groupId, target, false);
      }
      consumer.groupId = source;
      if (!group.consumerIds.includes(target)) group.consumerIds.push(target);
      rebalanceGroup(draft, source, shouldLog);
    }

    if (shouldLog) {
      logEvent(
        draft,
        "connect",
        `${getEntityName(draft, source)} 已连接到 ${getEntityName(draft, target)}，关系：${connectionLabel(draft, connection)}。`
      );
    }
    return true;
  }

  function removeConnection(draft, connectionId, shouldLog) {
    const index = draft.connections.findIndex((edge) => edge.id === connectionId);
    if (index < 0) return;
    const [connection] = draft.connections.splice(index, 1);
    const sourceNode = getNode(draft, connection.source);
    const targetNode = getNode(draft, connection.target);

    if (sourceNode?.type === "topic" && targetNode?.type === "group") {
      const group = draft.groups[connection.target];
      group.subscribedTopicIds = group.subscribedTopicIds.filter((id) => id !== connection.source);
      rebalanceGroup(draft, connection.target, shouldLog);
    }

    if (sourceNode?.type === "group" && targetNode?.type === "consumer") {
      removeConsumerFromGroup(draft, connection.source, connection.target, shouldLog);
    }

    if (shouldLog) {
      logEvent(draft, "connect", `已移除连接：${getEntityName(draft, connection.source)} -> ${getEntityName(draft, connection.target)}。`);
    }
  }

  function removeConsumerFromGroup(draft, groupId, consumerId, shouldLog) {
    const group = draft.groups[groupId];
    const consumer = draft.consumers[consumerId];
    if (!group || !consumer) return;
    group.consumerIds = group.consumerIds.filter((id) => id !== consumerId);
    delete group.assignments[consumerId];
    if (consumer.groupId === groupId) consumer.groupId = null;
    rebalanceGroup(draft, groupId, shouldLog);
  }

  function deleteNode(draft, nodeId) {
    const node = getNode(draft, nodeId);
    if (!node) return;
    const relatedConnections = draft.connections
      .filter((edge) => edge.source === nodeId || edge.target === nodeId)
      .map((edge) => edge.id);
    relatedConnections.forEach((edgeId) => removeConnection(draft, edgeId, false));

    draft.nodes = draft.nodes.filter((item) => item.id !== nodeId);
    delete draft.producers[nodeId];
    delete draft.brokers[nodeId];

    if (node.type === "topic") {
      delete draft.topics[nodeId];
      Object.keys(draft.groups).forEach((groupId) => {
        draft.groups[groupId].subscribedTopicIds = draft.groups[groupId].subscribedTopicIds.filter((id) => id !== nodeId);
        rebalanceGroup(draft, groupId, false);
      });
    }

    if (node.type === "group") {
      const group = draft.groups[nodeId];
      if (group) {
        group.consumerIds.forEach((consumerId) => {
          if (draft.consumers[consumerId]) draft.consumers[consumerId].groupId = null;
        });
      }
      delete draft.groups[nodeId];
    }

    if (node.type === "consumer") {
      const consumer = draft.consumers[nodeId];
      if (consumer?.groupId) removeConsumerFromGroup(draft, consumer.groupId, nodeId, false);
      delete draft.consumers[nodeId];
    }

    if (draft.selectedId === nodeId) draft.selectedId = null;
    logEvent(draft, "delete", `${TYPE_LABELS[node.type]}已删除。`);
  }

  function resizeTopic(draft, topicId, partitionCount) {
    const topic = draft.topics[topicId];
    if (!topic) return false;
    const hasRecords = topic.partitions.some((partition) => partition.records.length > 0);
    if (hasRecords) {
      logEvent(draft, "warn", "这个 Topic 已经有消息，当前版本不再允许修改 Partition 数。");
      return false;
    }

    const nextCount = Math.max(1, Math.min(12, Number(partitionCount) || 1));
    topic.partitionCount = nextCount;
    topic.partitions = Array.from({ length: nextCount }, (_, index) => topic.partitions[index] || createPartition(index));
    Object.keys(draft.groups).forEach((groupId) => {
      if (draft.groups[groupId].subscribedTopicIds.includes(topicId)) rebalanceGroup(draft, groupId, true);
    });
    logEvent(draft, "topic", `${topic.name} 现在有 ${nextCount} 个 Partition。`);
    return true;
  }

  function topicPartitionKey(topicId, partitionId) {
    return `${topicId}:${partitionId}`;
  }

  function parseTopicPartitionKey(key) {
    const parts = key.split(":");
    return {
      topicId: parts[0],
      partitionId: Number(parts[1])
    };
  }

  function hashString(value) {
    let hash = 2166136261;
    const text = String(value || "");
    for (let index = 0; index < text.length; index += 1) {
      hash ^= text.charCodeAt(index);
      hash = Math.imul(hash, 16777619);
    }
    return hash >>> 0;
  }

  function choosePartition(draft, producerId, topicId, explicitPartition) {
    const producer = draft.producers[producerId];
    const topic = draft.topics[topicId];
    const count = topic.partitions.length;
    const strategy = producer.strategy || "key-hash";

    if (strategy === "explicit") {
      const partitionId = Math.max(0, Math.min(count - 1, Number(explicitPartition) || 0));
      return { partitionId, reason: `指定 Partition ${partitionId}` };
    }

    if (strategy === "round-robin") {
      const partitionId = producer.roundRobinIndex % count;
      producer.roundRobinIndex += 1;
      return { partitionId, reason: `轮询到 Partition ${partitionId}` };
    }

    return null;
  }

  function produceRecord(draft, payload, shouldLog) {
    const producer = draft.producers[payload.producerId];
    const topic = draft.topics[payload.topicId];
    if (!producer || !topic) return null;

    let choice = choosePartition(draft, producer.id, topic.id, payload.explicitPartition);
    const keyText = String(payload.key || "");
    if (!choice) {
      const partitionId = keyText
        ? hashString(keyText) % topic.partitions.length
        : producer.roundRobinIndex++ % topic.partitions.length;
      choice = {
        partitionId,
        reason: keyText ? `Key 哈希("${keyText}")` : `未填写 Key，按轮询 Partition`
      };
    }

    const partition = topic.partitions[choice.partitionId];
    const record = {
      id: `record-${draft.nextRecordSeq}`,
      offset: partition.nextOffset,
      key: keyText,
      value: String(payload.value || ""),
      headers: String(payload.headers || ""),
      timestamp: new Date().toISOString(),
      producerId: producer.id,
      partitionId: choice.partitionId
    };
    draft.nextRecordSeq += 1;
    partition.records.push(record);
    partition.nextOffset += 1;
    topic.lastRecordId = record.id;
    producer.lastSend = {
      topicId: topic.id,
      partitionId: choice.partitionId,
      offset: record.offset,
      reason: choice.reason,
      key: record.key,
      value: record.value,
      headers: record.headers
    };

    if (shouldLog) {
      logEvent(
        draft,
        "produce",
        `${producer.name} 已写入 ${topic.name}[${choice.partitionId}]@${record.offset}；Partition 选择依据：${choice.reason}；数据：${recordDataText(record)}。`
      );
    }
    return record;
  }

  function recordDataText(record) {
    const parts = [
      `Key=${record.key || "null"}`,
      `Value=${shortText(record.value || "", 80)}`
    ];
    if (record.headers) parts.push(`Headers=${shortText(record.headers, 80)}`);
    return parts.join(", ");
  }

  function rebalanceGroup(draft, groupId, shouldLog) {
    const group = draft.groups[groupId];
    if (!group) return;

    group.consumerIds = unique(group.consumerIds).filter((consumerId) => draft.consumers[consumerId]?.groupId === groupId);
    group.subscribedTopicIds = unique(group.subscribedTopicIds).filter((topicId) => Boolean(draft.topics[topicId]));

    const assignments = {};
    group.consumerIds.forEach((consumerId) => {
      assignments[consumerId] = [];
    });

    const partitions = [];
    group.subscribedTopicIds.forEach((topicId) => {
      const topic = draft.topics[topicId];
      topic.partitions.forEach((partition) => {
        partitions.push({ topicId, partitionId: partition.id });
      });
    });

    if (group.consumerIds.length > 0) {
      partitions.forEach((partition, index) => {
        const consumerId = group.consumerIds[index % group.consumerIds.length];
        assignments[consumerId].push(partition);
      });
    }

    const activeKeys = new Set(partitions.map((partition) => topicPartitionKey(partition.topicId, partition.partitionId)));
    Object.keys(group.positions).forEach((key) => {
      if (!activeKeys.has(key)) delete group.positions[key];
    });
    Object.keys(group.committed).forEach((key) => {
      if (!activeKeys.has(key)) delete group.committed[key];
    });
    activeKeys.forEach((key) => {
      if (!Number.isFinite(group.positions[key])) group.positions[key] = group.committed[key] || 0;
      if (!Number.isFinite(group.committed[key])) group.committed[key] = 0;
    });

    group.assignments = assignments;
    if (shouldLog) {
      logEvent(
        draft,
        "rebalance",
        `${group.name} 已 Rebalance：${partitions.length} 个 Partition 分配给 ${group.consumerIds.length} 个 Consumer。`
      );
    }
  }

  function findNextRecordForGroup(draft, group, memberId) {
    const consumerIds = memberId ? [memberId] : group.consumerIds;
    for (const consumerId of consumerIds) {
      const assignments = group.assignments[consumerId] || [];
      for (const assignment of assignments) {
        const key = topicPartitionKey(assignment.topicId, assignment.partitionId);
        const position = group.positions[key] || 0;
        const partition = draft.topics[assignment.topicId]?.partitions[assignment.partitionId];
        const record = partition?.records.find((item) => item.offset === position);
        if (record) {
          return { consumerId, assignment, record };
        }
      }
    }
    return null;
  }

  function pollGroup(draft, groupId, maxRecords, memberId, shouldLog) {
    const group = draft.groups[groupId];
    if (!group) return [];
    rebalanceGroup(draft, groupId, false);

    const collected = [];
    const max = Math.max(1, Math.min(100, Number(maxRecords) || 1));
    for (let index = 0; index < max; index += 1) {
      const next = findNextRecordForGroup(draft, group, memberId);
      if (!next) break;
      const key = topicPartitionKey(next.assignment.topicId, next.assignment.partitionId);
      group.positions[key] = next.record.offset + 1;
      draft.consumers[next.consumerId].lastRecord = {
        topicId: next.assignment.topicId,
        partitionId: next.assignment.partitionId,
        offset: next.record.offset,
        value: next.record.value,
        key: next.record.key,
        headers: next.record.headers
      };
      collected.push(next);
      if (shouldLog) {
        logEvent(
          draft,
          "consume",
          `${draft.consumers[next.consumerId].name} 拉取了 ${draft.topics[next.assignment.topicId].name}[${next.assignment.partitionId}]@${next.record.offset}；数据：${recordDataText(next.record)}。`
        );
      }
    }

    if (shouldLog && collected.length === 0) {
      logEvent(draft, "consume", `${group.name} 在当前位置没有可消费的消息。`);
    }
    return collected;
  }

  function commitGroup(draft, groupId, shouldLog) {
    const group = draft.groups[groupId];
    if (!group) return;
    Object.keys(group.positions).forEach((key) => {
      group.committed[key] = group.positions[key];
    });
    if (shouldLog) {
      logEvent(draft, "commit", `${group.name} 已提交当前 Offset。已提交 Offset 的总 Lag 为 ${groupLag(draft, groupId).totalCommittedLag}。`);
    }
  }

  function resetGroup(draft, groupId, mode, shouldLog) {
    const group = draft.groups[groupId];
    if (!group) return;
    group.subscribedTopicIds.forEach((topicId) => {
      const topic = draft.topics[topicId];
      topic.partitions.forEach((partition) => {
        const key = topicPartitionKey(topicId, partition.id);
        group.positions[key] = mode === "latest" ? partition.nextOffset : 0;
      });
    });
    if (shouldLog) logEvent(draft, "seek", `${group.name} 已将 Consumer Group 的读取位置重置到${offsetResetLabel(mode)}，Topic 中的消息不会被修改。`);
  }

  function seekGroup(draft, groupId, topicId, partitionId, offset, shouldLog) {
    const group = draft.groups[groupId];
    const partition = draft.topics[topicId]?.partitions[partitionId];
    if (!group || !partition) return;
    const nextOffset = Math.max(0, Math.min(partition.nextOffset, Number(offset) || 0));
    group.positions[topicPartitionKey(topicId, partitionId)] = nextOffset;
    if (shouldLog) {
      logEvent(draft, "seek", `${group.name} 已将自己在 ${draft.topics[topicId].name}[${partitionId}] 上的读取位置移动到 Offset ${nextOffset}，Topic 中的消息不会被修改。`);
    }
  }

  function offsetResetLabel(mode) {
    return mode === "latest" ? "最新位置" : "最早位置";
  }

  function groupLag(draft, groupId) {
    const group = draft.groups[groupId];
    const rows = [];
    let totalCommittedLag = 0;
    let totalCurrentLag = 0;
    if (!group) return { rows, totalCommittedLag, totalCurrentLag };

    group.subscribedTopicIds.forEach((topicId) => {
      const topic = draft.topics[topicId];
      if (!topic) return;
      topic.partitions.forEach((partition) => {
        const key = topicPartitionKey(topicId, partition.id);
        const committed = group.committed[key] || 0;
        const position = group.positions[key] || 0;
        const end = partition.nextOffset;
        const assignedConsumerId = Object.keys(group.assignments).find((consumerId) => {
          return (group.assignments[consumerId] || []).some((item) => item.topicId === topicId && item.partitionId === partition.id);
        });
        const committedLag = Math.max(0, end - committed);
        const currentLag = Math.max(0, end - position);
        totalCommittedLag += committedLag;
        totalCurrentLag += currentLag;
        rows.push({
          key,
          topicId,
          topicName: topic.name,
          partitionId: partition.id,
          end,
          position,
          committed,
          currentLag,
          committedLag,
          assignedConsumerId,
          assignedConsumerName: assignedConsumerId ? draft.consumers[assignedConsumerId]?.name || "未知" : "未分配"
        });
      });
    });

    return { rows, totalCommittedLag, totalCurrentLag };
  }

  function logEvent(draft, type, message) {
    draft.events.push({
      id: `event-${draft.nextId}-${draft.events.length}`,
      timestamp: new Date().toISOString(),
      type,
      message
    });
    if (draft.events.length > 300) draft.events = draft.events.slice(draft.events.length - 300);
  }

  function unique(items) {
    return Array.from(new Set(items));
  }

  function connectedTopicsForProducer(draft, producerId) {
    return draft.connections
      .filter((edge) => edge.source === producerId && getNode(draft, edge.target)?.type === "topic")
      .map((edge) => edge.target);
  }

  function connectedGroupsForTopic(draft, topicId) {
    return draft.connections
      .filter((edge) => edge.source === topicId && getNode(draft, edge.target)?.type === "group")
      .map((edge) => edge.target);
  }

  function subscribedTopicsForGroup(draft, groupId) {
    return draft.groups[groupId]?.subscribedTopicIds || [];
  }

  function autoPollConnectedGroups(draft, topicId) {
    const consumed = [];
    connectedGroupsForTopic(draft, topicId).forEach((groupId) => {
      const group = draft.groups[groupId];
      if (!group || !group.consumerIds.length) return;
      const polled = pollGroup(draft, groupId, 1, null, true);
      if (polled.length) {
        consumed.push({ groupId, records: polled });
      }
    });
    if (!consumed.length) {
      logEvent(draft, "consume", "自动拉取没有找到可接收这条消息的 Consumer。");
    }
    return consumed;
  }

  function assignedRowsForConsumer(draft, consumerId) {
    const consumer = draft.consumers[consumerId];
    const group = consumer?.groupId ? draft.groups[consumer.groupId] : null;
    if (!group) return [];
    return (group.assignments[consumerId] || []).map((assignment) => {
      const key = topicPartitionKey(assignment.topicId, assignment.partitionId);
      return {
        ...assignment,
        topicName: draft.topics[assignment.topicId]?.name || "未知",
        position: group.positions[key] || 0,
        committed: group.committed[key] || 0,
        end: draft.topics[assignment.topicId]?.partitions[assignment.partitionId]?.nextOffset || 0
      };
    });
  }

  function serializeState(draft) {
    return JSON.stringify(draft, null, 2);
  }

  function normalizeState(input) {
    const normalized = createEmptyState();
    const imported = input && typeof input === "object" ? input : {};
    Object.assign(normalized, imported);
    normalized.nodes = Array.isArray(imported.nodes) ? imported.nodes : [];
    normalized.connections = Array.isArray(imported.connections) ? imported.connections : [];
    normalized.producers = imported.producers || {};
    normalized.topics = imported.topics || {};
    normalized.groups = imported.groups || {};
    normalized.consumers = imported.consumers || {};
    normalized.brokers = imported.brokers || {};
    normalized.events = Array.isArray(imported.events) ? imported.events : [];
    normalized.nextId = Number(imported.nextId) || normalized.nodes.length + normalized.connections.length + 1;
    normalized.nextRecordSeq = Number(imported.nextRecordSeq) || 1;
    normalized.selectedId = imported.selectedId && getNode(normalized, imported.selectedId) ? imported.selectedId : null;

    Object.values(normalized.topics).forEach((topic) => {
      topic.partitions = Array.isArray(topic.partitions) && topic.partitions.length ? topic.partitions : [createPartition(0)];
      topic.partitionCount = topic.partitions.length;
      topic.partitions.forEach((partition, index) => {
        partition.id = Number.isFinite(partition.id) ? partition.id : index;
        partition.records = Array.isArray(partition.records) ? partition.records : [];
        partition.nextOffset = Number.isFinite(partition.nextOffset) ? partition.nextOffset : partition.records.length;
      });
    });

    Object.values(normalized.producers).forEach((producer) => {
      producer.draftMessage = {
        ...DEFAULT_PRODUCER_DRAFT,
        ...(producer.draftMessage || {})
      };
    });

    Object.keys(normalized.groups).forEach((groupId) => rebalanceGroup(normalized, groupId, false));
    return normalized;
  }

  function saveState(shouldLog) {
    try {
      localStorage.setItem(STORAGE_KEY, serializeState(state));
      if (shouldLog) {
        logEvent(state, "save", "场景已保存到本地浏览器。");
        render();
      }
    } catch (error) {
      logEvent(state, "warn", `保存失败：${error.message}`);
      render();
    }
  }

  function loadSavedState() {
    const saved = localStorage.getItem(STORAGE_KEY);
    if (!saved) {
      logEvent(state, "warn", "本地浏览器里没有找到已保存的场景。");
      render();
      return;
    }
    try {
      state = normalizeState(JSON.parse(saved));
      clearFlowLayer();
      logEvent(state, "load", "已从本地浏览器读取保存的场景。");
      render();
    } catch (error) {
      logEvent(state, "warn", `读取保存的场景失败：${error.message}`);
      render();
    }
  }

  function openJsonModal(mode) {
    lastModalMode = mode;
    const modal = document.getElementById("jsonModal");
    const title = document.getElementById("jsonModalTitle");
    const text = document.getElementById("jsonText");
    title.textContent = mode === "import" ? "导入场景 JSON" : "导出场景 JSON";
    text.value = mode === "import" ? "" : serializeState(state);
    modal.classList.remove("hidden");
    text.focus();
  }

  function closeJsonModal() {
    document.getElementById("jsonModal").classList.add("hidden");
  }

  function applyJsonImport() {
    const text = document.getElementById("jsonText").value;
    try {
      state = normalizeState(JSON.parse(text));
      clearFlowLayer();
      logEvent(state, "load", "已从 JSON 导入场景。");
      closeJsonModal();
      render();
    } catch (error) {
      window.alert(`导入失败：${error.message}`);
    }
  }

  function copyJson() {
    const text = document.getElementById("jsonText").value;
    if (navigator.clipboard?.writeText) {
      navigator.clipboard.writeText(text).catch(() => {});
    }
  }

  function escapeHtml(value) {
    return String(value ?? "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#39;");
  }

  function shortText(value, limit) {
    const text = String(value ?? "");
    return text.length > limit ? `${text.slice(0, limit - 1)}...` : text;
  }

  function eventTypeLabel(type) {
    return EVENT_TYPE_LABELS[type] || type;
  }

  function formatTime(iso) {
    const date = new Date(iso);
    return date.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
  }

  function render() {
    if (typeof document === "undefined") return;
    applyCanvasZoom();
    renderNodes();
    updateCanvasLayerSize();
    renderEdges();
    renderProperties();
    renderLog();
  }

  function applyCanvasZoom() {
    const canvas = document.getElementById("canvas");
    if (canvas) canvas.style.setProperty("--canvas-zoom", String(canvasZoom));
    updateZoomControls();
  }

  function updateZoomControls() {
    const label = document.getElementById("zoomLabel");
    if (label) label.textContent = `${Math.round(canvasZoom * 100)}%`;
    const zoomOut = document.getElementById("zoomOutBtn");
    const zoomIn = document.getElementById("zoomInBtn");
    if (zoomOut) zoomOut.disabled = canvasZoom <= CANVAS_ZOOM_MIN;
    if (zoomIn) zoomIn.disabled = canvasZoom >= CANVAS_ZOOM_MAX;
  }

  function setCanvasZoom(nextZoom) {
    const canvas = document.getElementById("canvas");
    const previousZoom = canvasZoom;
    const centerWorld = canvas
      ? {
          x: (canvas.scrollLeft + canvas.clientWidth / 2) / previousZoom,
          y: (canvas.scrollTop + canvas.clientHeight / 2) / previousZoom
        }
      : null;
    canvasZoom = clampZoom(nextZoom);
    render();
    if (canvas && centerWorld) {
      canvas.scrollLeft = Math.max(0, centerWorld.x * canvasZoom - canvas.clientWidth / 2);
      canvas.scrollTop = Math.max(0, centerWorld.y * canvasZoom - canvas.clientHeight / 2);
    }
  }

  function clampZoom(value) {
    const stepped = Math.round(Number(value) * 10) / 10;
    return Math.max(CANVAS_ZOOM_MIN, Math.min(CANVAS_ZOOM_MAX, stepped || 1));
  }

  function updateCanvasLayerSize() {
    const canvas = document.getElementById("canvas");
    if (!canvas) return;
    const layers = [document.getElementById("edgeLayer"), document.getElementById("nodeLayer"), document.getElementById("flowLayer")];
    let maxRight = canvas.clientWidth;
    let maxBottom = canvas.clientHeight;
    document.querySelectorAll(".node").forEach((nodeElement) => {
      maxRight = Math.max(maxRight, nodeElement.offsetLeft + nodeElement.offsetWidth + 180 * canvasZoom);
      maxBottom = Math.max(maxBottom, nodeElement.offsetTop + nodeElement.offsetHeight + 160 * canvasZoom);
    });
    const width = `${Math.ceil(maxRight)}px`;
    const height = `${Math.ceil(maxBottom)}px`;
    layers.forEach((layer) => {
      if (!layer) return;
      layer.style.width = width;
      layer.style.height = height;
    });
  }

  function renderNodes() {
    const layer = document.getElementById("nodeLayer");
    layer.innerHTML = "";
    state.nodes.forEach((node) => {
      const entity = getEntity(state, node.id);
      const element = document.createElement("div");
      element.className = `node ${node.type}${state.selectedId === node.id ? " selected" : ""}`;
      element.dataset.id = node.id;
      element.style.left = `${node.x * canvasZoom}px`;
      element.style.top = `${node.y * canvasZoom}px`;
      element.innerHTML = `
        ${canReceiveConnectionType(node.type) ? `<button class="connector-handle connector-in" type="button" title="拖线到这里完成连接" aria-label="连接入口"></button>` : ""}
        ${canStartConnectionType(node.type) ? `<button class="connector-handle connector-out" type="button" title="从这里拖出连线" aria-label="连接出口"></button>` : ""}
        <div class="node-header">
          <span class="type-badge ${node.type}">${TYPE_INITIALS[node.type]}</span>
          <div class="node-title">${escapeHtml(entity?.name || TYPE_LABELS[node.type])}</div>
        </div>
        <div class="node-meta">${nodeMetaHtml(node)}</div>
      `;
      element.addEventListener("pointerdown", onNodePointerDown);
      element.querySelector(".connector-out")?.addEventListener("pointerdown", onConnectorPointerDown);
      element.querySelector(".connector-in")?.addEventListener("pointerdown", (event) => {
        event.preventDefault();
        event.stopPropagation();
      });
      layer.appendChild(element);
    });
  }

  function canStartConnectionType(type) {
    return type === "producer" || type === "topic" || type === "group";
  }

  function canReceiveConnectionType(type) {
    return type === "topic" || type === "group" || type === "consumer";
  }

  function nodeMetaHtml(node) {
    if (node.type === "producer") {
      const producer = state.producers[node.id];
      const topics = connectedTopicsForProducer(state, node.id).length;
      const last = producer.lastSend
        ? `${state.topics[producer.lastSend.topicId]?.name || "Topic"}[${producer.lastSend.partitionId}]@${producer.lastSend.offset} ${recordDataText(producer.lastSend)}`
        : "暂无";
      return `
        Partition 策略：${escapeHtml(STRATEGY_LABELS[producer.strategy])}<br>
        已连接 Topic：${topics}<br>
        最近发送：${escapeHtml(last)}
      `;
    }

    if (node.type === "topic") {
      const topic = state.topics[node.id];
      const records = topic.partitions.reduce((sum, partition) => sum + partition.records.length, 0);
      return `
        Partition 数：${topic.partitions.length}<br>
        消息数：${records}
        <div class="node-kpis">${topic.partitions.map((partition) => `<span class="kpi">p${partition.id}: ${partition.records.length}</span>`).join("")}</div>
      `;
    }

    if (node.type === "group") {
      const group = state.groups[node.id];
      const lag = groupLag(state, node.id);
      return `
        订阅 Topic：${group.subscribedTopicIds.length}<br>
        Consumer：${group.consumerIds.length}<br>
        已提交 Lag：${lag.totalCommittedLag}
      `;
    }

    if (node.type === "consumer") {
      const consumer = state.consumers[node.id];
      const assignedRows = assignedRowsForConsumer(state, node.id);
      const assignmentSummary = consumerAssignmentSummary(assignedRows);
      const last = consumer.lastRecord
        ? `${state.topics[consumer.lastRecord.topicId]?.name || "Topic"}[${consumer.lastRecord.partitionId}]@${consumer.lastRecord.offset} ${recordDataText(consumer.lastRecord)}`
        : "暂无";
      return `
        所属 Consumer Group：${escapeHtml(consumer.groupId ? getEntityName(state, consumer.groupId) : "暂无")}<br>
        负责 Partition：${escapeHtml(assignmentSummary)}<br>
        最近收到：${escapeHtml(last)}
      `;
    }

    const topicCount = Object.keys(state.topics).length;
    const partitionCount = Object.values(state.topics).reduce((sum, topic) => sum + topic.partitions.length, 0);
    return `Topic 数：${topicCount}<br>Partition 数：${partitionCount}`;
  }

  function consumerAssignmentSummary(rows) {
    if (!rows.length) return "暂无";
    const partitions = rows.map((row) => `${row.topicName}[${row.partitionId}]`);
    return shortText(`${rows.length} 个：${partitions.join(", ")}`, 34);
  }

  function renderEdges() {
    const svg = document.getElementById("edgeLayer");
    const existingDefs = svg.querySelector("defs");
    svg.innerHTML = "";
    if (existingDefs) svg.appendChild(existingDefs);

    state.connections.forEach((edge) => {
      const sourceElement = document.querySelector(`.node[data-id="${edge.source}"]`);
      const targetElement = document.querySelector(`.node[data-id="${edge.target}"]`);
      if (!sourceElement || !targetElement) return;
      const x1 = sourceElement.offsetLeft + sourceElement.offsetWidth / 2;
      const y1 = sourceElement.offsetTop + sourceElement.offsetHeight / 2;
      const x2 = targetElement.offsetLeft + targetElement.offsetWidth / 2;
      const y2 = targetElement.offsetTop + targetElement.offsetHeight / 2;

      const line = document.createElementNS("http://www.w3.org/2000/svg", "line");
      line.setAttribute("x1", x1);
      line.setAttribute("y1", y1);
      line.setAttribute("x2", x2);
      line.setAttribute("y2", y2);
      svg.appendChild(line);

      const label = document.createElementNS("http://www.w3.org/2000/svg", "text");
      label.setAttribute("x", (x1 + x2) / 2 + 4);
      label.setAttribute("y", (y1 + y2) / 2 - 4);
      label.setAttribute("class", "edge-label");
      label.textContent = connectionLabel(state, edge);
      svg.appendChild(label);
    });
  }

  function canAnimateFlow() {
    if (typeof document === "undefined") return false;
    if (!document.getElementById("flowLayer")) return false;
    return !window.matchMedia?.("(prefers-reduced-motion: reduce)")?.matches;
  }

  function enqueueFlow(runner) {
    flowQueue = flowQueue
      .catch(() => {})
      .then(() => {
        if (!canAnimateFlow()) return undefined;
        return runner();
      })
      .catch(() => {});
    return flowQueue;
  }

  function delay(ms) {
    return new Promise((resolve) => window.setTimeout(resolve, ms));
  }

  function canvasPointForNode(nodeId, xRatio, yRatio) {
    const element = document.querySelector(`.node[data-id="${nodeId}"]`);
    if (!element) return null;
    return {
      x: element.offsetLeft + element.offsetWidth * xRatio,
      y: element.offsetTop + element.offsetHeight * yRatio
    };
  }

  function topicPartitionPoint(topicId, partitionId) {
    const topic = state.topics[topicId];
    const element = document.querySelector(`.node[data-id="${topicId}"]`);
    if (!topic || !element) return canvasPointForNode(topicId, 0.5, 0.5);
    const count = Math.max(1, topic.partitions.length);
    const slot = Math.max(0, Math.min(count - 1, Number(partitionId) || 0));
    const yRatio = count === 1 ? 0.68 : 0.56 + (slot / Math.max(1, count - 1)) * 0.24;
    return {
      x: element.offsetLeft + element.offsetWidth * 0.86,
      y: element.offsetTop + element.offsetHeight * yRatio
    };
  }

  function pathWithMidpoint(points, bend) {
    if (points.length !== 2) return points;
    const [start, end] = points;
    return [
      start,
      {
        x: (start.x + end.x) / 2,
        y: (start.y + end.y) / 2 + bend
      },
      end
    ];
  }

  function pulseNode(nodeId, kind) {
    const element = document.querySelector(`.node[data-id="${nodeId}"]`);
    if (!element) return;
    const className = `flow-pulse-${kind}`;
    element.classList.remove(className);
    void element.offsetWidth;
    element.classList.add(className);
    window.setTimeout(() => element.classList.remove(className), 820);
  }

  function markFlowAnimation(kind) {
    const current = Number(document.body.dataset.flowAnimations || "0");
    document.body.dataset.flowAnimations = String(current + 1);
    document.body.dataset.lastFlowKind = kind;
  }

  function animatePacket(points, options) {
    const flowLayer = document.getElementById("flowLayer");
    const usablePoints = points.filter(Boolean);
    if (!flowLayer || usablePoints.length < 2) return Promise.resolve();

    const packet = document.createElement("div");
    packet.className = `flow-packet ${options.kind}`;
    packet.textContent = shortText(options.label || "消息", 18);
    flowLayer.appendChild(packet);
    markFlowAnimation(options.kind);

    const lastIndex = Math.max(1, usablePoints.length - 1);
    const keyframes = usablePoints.map((point, index) => ({
      transform: `translate(${point.x - 17 * canvasZoom}px, ${point.y - 14 * canvasZoom}px) scale(${index === lastIndex ? 0.92 : 1})`,
      opacity: index === 0 ? 0.2 : index === lastIndex ? 0.35 : 1,
      offset: index / lastIndex
    }));

    const animation = packet.animate(keyframes, {
      duration: options.duration || FLOW_DURATIONS[options.kind] || 800,
      easing: "cubic-bezier(.2,.7,.2,1)",
      fill: "forwards"
    });

    return animation.finished
      .catch(() => {})
      .then(() => {
        packet.remove();
      });
  }

  function animateProducedRecord(producerId, topicId, record) {
    if (!record) return;
    enqueueFlow(async () => {
      const start = canvasPointForNode(producerId, 1, 0.5);
      const end = topicPartitionPoint(topicId, record.partitionId);
      pulseNode(producerId, "produce");
      await animatePacket(pathWithMidpoint([start, end], -28), {
        kind: "produce",
        label: `${record.key || "null"} @${record.offset}`,
        duration: FLOW_DURATIONS.produce
      });
      pulseNode(topicId, "produce");
    });
  }

  function animateConsumedRecords(items, groupId) {
    if (!items || !items.length) return;
    enqueueFlow(async () => {
      for (const item of items) {
        const topicId = item.assignment.topicId;
        const topicPoint = topicPartitionPoint(topicId, item.assignment.partitionId);
        const groupIn = canvasPointForNode(groupId, 0.5, 0.18);
        const groupOut = canvasPointForNode(groupId, 0.08, 0.5);
        const consumerIn = canvasPointForNode(item.consumerId, 1, 0.5);
        pulseNode(topicId, "consume");
        await animatePacket([topicPoint, groupIn, groupOut, consumerIn], {
          kind: "consume",
          label: `@${item.record.offset}`,
          duration: FLOW_DURATIONS.consume
        });
        pulseNode(groupId, "consume");
        pulseNode(item.consumerId, "consume");
        await delay(110);
      }
    });
  }

  function animateCommitFlow(groupId) {
    const group = state.groups[groupId];
    if (!group || !group.consumerIds.length) {
      pulseNode(groupId, "commit");
      return;
    }
    enqueueFlow(async () => {
      const groupPoint = canvasPointForNode(groupId, 0.5, 0.5);
      const animations = group.consumerIds.map((consumerId, index) => {
        return delay(index * 120).then(() => animatePacket([
          canvasPointForNode(consumerId, 1, 0.5),
          groupPoint
        ], {
          kind: "commit",
          label: "提交",
          duration: FLOW_DURATIONS.commit
        }));
      });
      await Promise.all(animations);
      pulseNode(groupId, "commit");
    });
  }

  function animateRebalanceFlow(groupId) {
    const group = state.groups[groupId];
    if (!group || !group.consumerIds.length) {
      pulseNode(groupId, "rebalance");
      return;
    }
    enqueueFlow(async () => {
      const start = canvasPointForNode(groupId, 0.5, 0.5);
      const animations = group.consumerIds.map((consumerId, index) => {
        const assignments = group.assignments[consumerId] || [];
        const label = assignments.length ? `${assignments.length} Partition` : "空闲";
        return delay(index * 120).then(() => animatePacket([
          start,
          canvasPointForNode(consumerId, 0.5, 0.2)
        ], {
          kind: "rebalance",
          label,
          duration: FLOW_DURATIONS.rebalance
        }));
      });
      await Promise.all(animations);
      pulseNode(groupId, "rebalance");
      group.consumerIds.forEach((consumerId) => pulseNode(consumerId, "rebalance"));
    });
  }

  function clearFlowLayer() {
    const flowLayer = document.getElementById("flowLayer");
    if (flowLayer) flowLayer.innerHTML = "";
  }

  function updateNodeSelectionVisual() {
    document.querySelectorAll(".node").forEach((element) => {
      element.classList.toggle("selected", element.dataset.id === state.selectedId);
    });
  }

  function onNodePointerDown(event) {
    if (event.target.closest(".connector-handle")) return;
    const nodeElement = event.currentTarget;
    const nodeId = nodeElement.dataset.id;
    const node = getNode(state, nodeId);
    if (!node) return;
    state.selectedId = nodeId;
    renderProperties();
    updateNodeSelectionVisual();

    const pointer = canvasPointFromEvent(event);
    const startX = event.clientX;
    const startY = event.clientY;
    dragState = {
      nodeId,
      startX,
      startY,
      offsetX: pointer.x - node.x * canvasZoom,
      offsetY: pointer.y - node.y * canvasZoom,
      moved: false
    };
    if (typeof nodeElement.setPointerCapture === "function") {
      try {
        nodeElement.setPointerCapture(event.pointerId);
      } catch (error) {
        // Some automation/browser surfaces dispatch pointer-like events that cannot be captured.
      }
    }
    document.addEventListener("pointermove", onNodePointerMove);
    document.addEventListener("pointerup", onNodePointerUp, { once: true });
  }

  function onNodePointerMove(event) {
    if (!dragState) return;
    const node = getNode(state, dragState.nodeId);
    if (!node) return;
    if (Math.abs(event.clientX - dragState.startX) > 3 || Math.abs(event.clientY - dragState.startY) > 3) {
      dragState.moved = true;
    }
    const pointer = canvasPointFromEvent(event);
    node.x = Math.max(8, (pointer.x - dragState.offsetX) / canvasZoom);
    node.y = Math.max(8, (pointer.y - dragState.offsetY) / canvasZoom);
    const nodeElement = document.querySelector(`.node[data-id="${node.id}"]`);
    if (nodeElement) {
      nodeElement.style.left = `${node.x * canvasZoom}px`;
      nodeElement.style.top = `${node.y * canvasZoom}px`;
    }
    updateCanvasLayerSize();
    renderEdges();
  }

  function onNodePointerUp(event) {
    document.removeEventListener("pointermove", onNodePointerMove);
    dragState = null;
    saveState(false);
  }

  function onConnectorPointerDown(event) {
    event.preventDefault();
    event.stopPropagation();
    const nodeElement = event.currentTarget.closest(".node");
    const sourceId = nodeElement?.dataset.id;
    if (!sourceId || !getNode(state, sourceId)) return;

    state.selectedId = sourceId;
    renderProperties();
    updateNodeSelectionVisual();

    const line = createConnectionPreviewLine(sourceId, event);
    connectState = {
      sourceId,
      line,
      targetId: null
    };
    document.getElementById("canvas")?.classList.add("connecting");
    document.addEventListener("pointermove", onConnectorPointerMove);
    document.addEventListener("pointerup", onConnectorPointerUp, { once: true });
  }

  function createConnectionPreviewLine(sourceId, event) {
    const svg = document.getElementById("edgeLayer");
    if (!svg) return null;
    const start = canvasPointForNode(sourceId, 1, 0.5);
    const end = canvasPointFromEvent(event);
    const line = document.createElementNS("http://www.w3.org/2000/svg", "line");
    line.setAttribute("class", "edge-preview");
    line.setAttribute("x1", start?.x || 0);
    line.setAttribute("y1", start?.y || 0);
    line.setAttribute("x2", end.x);
    line.setAttribute("y2", end.y);
    svg.appendChild(line);
    return line;
  }

  function canvasPointFromEvent(event) {
    const canvas = document.getElementById("canvas");
    const rect = canvas.getBoundingClientRect();
    return {
      x: event.clientX - rect.left + canvas.scrollLeft,
      y: event.clientY - rect.top + canvas.scrollTop
    };
  }

  function onConnectorPointerMove(event) {
    if (!connectState) return;
    const end = canvasPointFromEvent(event);
    if (connectState.line) {
      connectState.line.setAttribute("x2", end.x);
      connectState.line.setAttribute("y2", end.y);
    }
    const targetId = nodeIdAtPoint(event.clientX, event.clientY);
    connectState.targetId = targetId && targetId !== connectState.sourceId ? targetId : null;
    updateConnectionTargetVisual();
  }

  function onConnectorPointerUp(event) {
    document.removeEventListener("pointermove", onConnectorPointerMove);
    if (!connectState) return;

    const sourceId = connectState.sourceId;
    const targetId = nodeIdAtPoint(event.clientX, event.clientY);
    cleanupConnectionPreview();

    if (!targetId || targetId === sourceId) {
      renderProperties();
      return;
    }

    const connected = connectNodes(state, sourceId, targetId, true);
    render();
    if (connected) {
      const sourceType = getNode(state, sourceId)?.type;
      const targetType = getNode(state, targetId)?.type;
      if (sourceType === "topic" && targetType === "group") animateRebalanceFlow(targetId);
      if (sourceType === "group" && targetType === "consumer") animateRebalanceFlow(sourceId);
    }
  }

  function nodeIdAtPoint(clientX, clientY) {
    const element = document.elementFromPoint(clientX, clientY);
    return element?.closest(".node")?.dataset.id || null;
  }

  function updateConnectionTargetVisual() {
    document.querySelectorAll(".node.connection-target").forEach((element) => {
      element.classList.remove("connection-target");
    });
    if (!connectState?.targetId) return;
    const target = document.querySelector(`.node[data-id="${connectState.targetId}"]`);
    if (target) target.classList.add("connection-target");
  }

  function cleanupConnectionPreview() {
    connectState?.line?.remove();
    connectState = null;
    document.getElementById("canvas")?.classList.remove("connecting");
    document.querySelectorAll(".node.connection-target").forEach((element) => {
      element.classList.remove("connection-target");
    });
  }

  function renderProperties() {
    const panel = document.getElementById("properties");
    const node = getNode(state, state.selectedId);
    if (!node) {
      renderOverview(panel);
      return;
    }

    if (node.type === "producer") renderProducerPanel(panel, node);
    if (node.type === "topic") renderTopicPanel(panel, node);
    if (node.type === "group") renderGroupPanel(panel, node);
    if (node.type === "consumer") renderConsumerPanel(panel, node);
    if (node.type === "broker") renderBrokerPanel(panel, node);
    wireCommonPanelActions(node);
  }

  function renderOverview(panel) {
    const totals = {
      producers: Object.keys(state.producers).length,
      topics: Object.keys(state.topics).length,
      groups: Object.keys(state.groups).length,
      consumers: Object.keys(state.consumers).length,
      records: Object.values(state.topics).reduce((sum, topic) => {
        return sum + topic.partitions.reduce((inner, partition) => inner + partition.records.length, 0);
      }, 0)
    };
    panel.innerHTML = `
      <div class="properties-section">
        <h2>工作台</h2>
        <p class="hint">选择一个节点查看属性，并执行发送、消费、提交 Offset 等模拟操作。</p>
        <div class="summary-grid">
          <div class="summary-card"><strong>${totals.producers}</strong><span>Producer</span></div>
          <div class="summary-card"><strong>${totals.topics}</strong><span>Topic</span></div>
          <div class="summary-card"><strong>${totals.groups}</strong><span>Consumer Group</span></div>
          <div class="summary-card"><strong>${totals.records}</strong><span>消息</span></div>
        </div>
      </div>
      <div class="properties-section">
        <div class="panel-title">消息中间件流程</div>
        <div class="notice">按 Producer -> Topic -> Consumer Group -> Consumer 连接。然后发送消息、拉取消息、提交 Offset、观察 Lag、触发 Rebalance 或回放历史消息。</div>
      </div>
      <div class="properties-section">
        <div class="panel-title">快速添加</div>
        <div class="button-row">
          ${Object.keys(TYPE_LABELS).map((type) => `<button class="quick-add" data-type="${type}" type="button">${TYPE_LABELS[type]}</button>`).join("")}
        </div>
      </div>
    `;
    panel.querySelectorAll(".quick-add").forEach((button) => {
      button.addEventListener("click", () => addNode(button.dataset.type));
    });
  }

  function commonNodeHtml(node) {
    const entity = getEntity(state, node.id);
    const targets = state.nodes.filter((target) => validateConnection(state, node.id, target.id) && !edgeExists(state, node.id, target.id));
    const connections = state.connections.filter((edge) => edge.source === node.id || edge.target === node.id);
    return `
      <div class="properties-section">
        <h2>${TYPE_LABELS[node.type]}</h2>
        <div class="field">
          <label for="nodeName">名称</label>
          <input id="nodeName" value="${escapeHtml(entity?.name || "")}">
        </div>
        <div class="button-row">
          <button id="deleteNodeBtn" class="danger" type="button">删除节点</button>
        </div>
      </div>
      <div class="properties-section">
        <div class="panel-title">连接</div>
        ${targets.length ? `
          <div class="field">
            <label for="connectTarget">将当前节点连接到</label>
            <select id="connectTarget">
              ${targets.map((target) => `<option value="${target.id}">${escapeHtml(getEntityName(state, target.id))} (${TYPE_LABELS[target.type]})</option>`).join("")}
            </select>
          </div>
          <div class="button-row"><button id="connectBtn" type="button">连接</button></div>
        ` : `<p class="hint">当前节点没有可新增的有效连接。</p>`}
        ${connections.length ? `
          <table class="mini-table">
            <thead><tr><th>来源</th><th>目标</th><th></th></tr></thead>
            <tbody>
              ${connections.map((edge) => `
                <tr>
                  <td>${escapeHtml(getEntityName(state, edge.source))}</td>
                  <td>${escapeHtml(getEntityName(state, edge.target))}</td>
                  <td><button class="compact remove-connection" data-edge="${edge.id}" type="button">移除</button></td>
                </tr>
              `).join("")}
            </tbody>
          </table>
        ` : ""}
      </div>
    `;
  }

  function wireCommonPanelActions(node) {
    const nameInput = document.getElementById("nodeName");
    if (nameInput) {
      nameInput.addEventListener("change", () => {
        setEntityName(state, node.id, nameInput.value);
        logEvent(state, "edit", `${TYPE_LABELS[node.type]}已重命名为 ${getEntityName(state, node.id)}。`);
        render();
      });
    }

    const deleteButton = document.getElementById("deleteNodeBtn");
    if (deleteButton) {
      deleteButton.addEventListener("click", () => {
        deleteNode(state, node.id);
        render();
      });
    }

    const connectButton = document.getElementById("connectBtn");
    if (connectButton) {
      connectButton.addEventListener("click", () => {
        const target = document.getElementById("connectTarget").value;
        const connected = connectNodes(state, node.id, target, true);
        render();
        if (connected) {
          const sourceType = getNode(state, node.id)?.type;
          const targetType = getNode(state, target)?.type;
          if (sourceType === "topic" && targetType === "group") animateRebalanceFlow(target);
          if (sourceType === "group" && targetType === "consumer") animateRebalanceFlow(node.id);
        }
      });
    }

    document.querySelectorAll(".remove-connection").forEach((button) => {
      button.addEventListener("click", () => {
        removeConnection(state, button.dataset.edge, true);
        render();
      });
    });
  }

  function renderProducerPanel(panel, node) {
    const producer = state.producers[node.id];
    const topics = connectedTopicsForProducer(state, node.id);
    panel.innerHTML = `
      ${commonNodeHtml(node)}
      <div class="properties-section">
        <div class="panel-title">发送消息</div>
        ${topics.length ? producerFormHtml(producer, topics) : `<div class="notice">先把这个 Producer 连接到 Topic，才能发送消息。</div>`}
      </div>
    `;
    const strategy = document.getElementById("producerStrategy");
    if (strategy) {
      strategy.addEventListener("change", () => {
        producer.strategy = strategy.value;
        logEvent(state, "edit", `${producer.name} 的 Partition 策略已设置为：${STRATEGY_LABELS[producer.strategy]}。`);
        render();
      });
    }
    wireProducerDraftInputs(producer);

    const sendButton = document.getElementById("sendRecordBtn");
    if (sendButton) {
      sendButton.addEventListener("click", () => {
        const value = document.getElementById("recordValue").value;
        const topicId = document.getElementById("targetTopic").value;
        if (!value.trim()) {
          logEvent(state, "warn", "消息内容不能为空。");
          render();
          return;
        }
        producer.draftMessage = {
          topicId,
          key: document.getElementById("recordKey").value,
          value,
          headers: document.getElementById("recordHeaders").value,
          explicitPartition: Number(document.getElementById("explicitPartition")?.value ?? producer.draftMessage.explicitPartition) || 0,
          autoPoll: Boolean(document.getElementById("autoPollAfterSend")?.checked)
        };
        const record = produceRecord(state, {
          producerId: producer.id,
          topicId,
          key: producer.draftMessage.key,
          value,
          headers: producer.draftMessage.headers,
          explicitPartition: producer.draftMessage.explicitPartition
        }, true);
        const autoPolled = producer.draftMessage.autoPoll ? autoPollConnectedGroups(state, topicId) : [];
        render();
        animateProducedRecord(producer.id, topicId, record);
        autoPolled.forEach((result) => animateConsumedRecords(result.records, result.groupId));
      });
    }
  }

  function wireProducerDraftInputs(producer) {
    const draft = producer.draftMessage || { ...DEFAULT_PRODUCER_DRAFT };
    const fields = [
      ["targetTopic", "topicId", (value) => value],
      ["recordKey", "key", (value) => value],
      ["recordValue", "value", (value) => value],
      ["recordHeaders", "headers", (value) => value],
      ["explicitPartition", "explicitPartition", (value) => Number(value) || 0],
      ["autoPollAfterSend", "autoPoll", (_value, element) => Boolean(element.checked)]
    ];
    fields.forEach(([elementId, field, normalize]) => {
      const element = document.getElementById(elementId);
      if (!element) return;
      element.addEventListener("input", () => {
        draft[field] = normalize(element.value, element);
        producer.draftMessage = draft;
      });
      element.addEventListener("change", () => {
        draft[field] = normalize(element.value, element);
        producer.draftMessage = draft;
      });
    });
  }

  function producerFormHtml(producer, topicIds) {
    const draft = {
      ...DEFAULT_PRODUCER_DRAFT,
      ...(producer.draftMessage || {})
    };
    const selectedTopicId = topicIds.includes(draft.topicId) ? draft.topicId : topicIds[0];
    producer.draftMessage = {
      ...draft,
      topicId: selectedTopicId
    };
    return `
      <div class="field">
        <label for="targetTopic">Topic</label>
        <select id="targetTopic">
          ${topicIds.map((topicId) => `<option value="${topicId}"${selectedTopicId === topicId ? " selected" : ""}>${escapeHtml(state.topics[topicId].name)}</option>`).join("")}
        </select>
      </div>
      <div class="field">
        <label for="producerStrategy">Partition 策略</label>
        <select id="producerStrategy">
          ${Object.entries(STRATEGY_LABELS).map(([value, label]) => `<option value="${value}"${producer.strategy === value ? " selected" : ""}>${label}</option>`).join("")}
        </select>
        <p class="hint">${partitionStrategyHint(producer.strategy)}</p>
      </div>
      <div class="form-row">
        <div class="field">
          <label for="recordKey">Key（用于 Partition 和顺序）</label>
          <input id="recordKey" placeholder="客户-42" value="${escapeHtml(draft.key)}">
        </div>
        ${producer.strategy === "explicit" ? `
          <div class="field">
            <label for="explicitPartition">指定 Partition</label>
            <input id="explicitPartition" type="number" min="0" value="${escapeHtml(draft.explicitPartition)}">
          </div>
        ` : ""}
      </div>
      <div class="field">
        <label for="recordValue">消息内容 Value</label>
        <input id="recordValue" placeholder="订单已创建" value="${escapeHtml(draft.value)}">
      </div>
      <div class="field">
        <label for="recordHeaders">消息头 Headers</label>
        <input id="recordHeaders" placeholder="traceId=abc,来源=网页" value="${escapeHtml(draft.headers)}">
      </div>
      <label class="check-row" for="autoPollAfterSend">
        <input id="autoPollAfterSend" type="checkbox"${draft.autoPoll ? " checked" : ""}>
        <span>发送后自动让已连接 Consumer 拉取一次</span>
      </label>
      <p class="hint">开启后，发送会先写入 Topic，再立即触发一次消费，让动画从 Producer 继续流到 Consumer。</p>
      <div class="button-row">
        <button id="sendRecordBtn" class="primary" type="button">发送消息</button>
      </div>
      ${producer.lastSend ? `<p class="hint">最近发送：${escapeHtml(state.topics[producer.lastSend.topicId]?.name || "Topic")}[${producer.lastSend.partitionId}]@${producer.lastSend.offset}；${escapeHtml(recordDataText(producer.lastSend))}；${escapeHtml(producer.lastSend.reason)}</p>` : ""}
    `;
  }

  function partitionStrategyHint(strategy) {
    if (strategy === "round-robin") {
      return "轮询 Partition 会把消息依次写入不同 Partition，多个 Consumer 更容易同时收到消息。";
    }
    if (strategy === "explicit") {
      return "指定 Partition 会把消息固定写入某个 Partition，只有负责该 Partition 的 Consumer 会收到。";
    }
    return "按 Key 哈希时，相同 Key 会一直进入同一 Partition；同一 Consumer Group 里只有负责该 Partition 的 Consumer 会收到。";
  }

  function renderTopicPanel(panel, node) {
    const topic = state.topics[node.id];
    const hasRecords = topic.partitions.some((partition) => partition.records.length > 0);
    panel.innerHTML = `
      ${commonNodeHtml(node)}
      <div class="properties-section">
        <div class="panel-title">Partition</div>
        <div class="field">
          <label for="partitionCount">Partition 数量</label>
          <input id="partitionCount" type="number" min="1" max="12" value="${topic.partitions.length}"${hasRecords ? " disabled" : ""}>
        </div>
        <div class="button-row">
          <button id="applyPartitionCountBtn" type="button"${hasRecords ? " disabled" : ""}>应用</button>
        </div>
        <p class="hint">${hasRecords ? "这个 Topic 已经有消息，因此 Partition 数量已锁定。" : "在写入消息前修改 Partition 数，可以保持学习模型清晰。写入后 Partition 数会锁定。"}</p>
      </div>
      <div class="properties-section">
        <div class="panel-title">追加日志（消费不会删除）</div>
        <p class="hint">Topic 里的消息是 append-only log；Consumer 只读取并移动自己的 Offset，不会修改这里的消息。</p>
        <div class="partition-list">
          ${topic.partitions.map((partition) => partitionHtml(topic, partition)).join("")}
        </div>
      </div>
    `;
    const applyButton = document.getElementById("applyPartitionCountBtn");
    if (applyButton) {
      applyButton.addEventListener("click", () => {
        resizeTopic(state, topic.id, document.getElementById("partitionCount").value);
        render();
      });
    }
  }

  function partitionHtml(topic, partition) {
    return `
      <div class="partition">
        <div class="partition-title">
          <span>${escapeHtml(topic.name)}[${partition.id}]</span>
          <span>末端 Offset ${partition.nextOffset}</span>
        </div>
        <div class="record-list">
          ${partition.records.length ? partition.records.map((record) => `
            <div class="record${topic.lastRecordId === record.id ? " latest" : ""}">
              <div class="record-offset">@${record.offset}</div>
              <div class="record-value">Key=${escapeHtml(record.key || "null")} 值=${escapeHtml(shortText(record.value, 48))}</div>
            </div>
          `).join("") : `<div class="record"><div class="record-offset">空</div><div class="record-value">暂无写入的消息</div></div>`}
        </div>
      </div>
    `;
  }

  function renderGroupPanel(panel, node) {
    const group = state.groups[node.id];
    const lag = groupLag(state, node.id);
    panel.innerHTML = `
      ${commonNodeHtml(node)}
      <div class="properties-section">
        <div class="panel-title">Offset 与 Lag</div>
        <div class="summary-grid">
          <div class="summary-card"><strong>${group.consumerIds.length}</strong><span>Consumer</span></div>
          <div class="summary-card"><strong>${group.subscribedTopicIds.length}</strong><span>Topic</span></div>
          <div class="summary-card"><strong>${lag.totalCurrentLag}</strong><span>当前位置 Lag</span></div>
          <div class="summary-card"><strong>${lag.totalCommittedLag}</strong><span>已提交 Lag</span></div>
        </div>
        ${lagTableHtml(lag.rows)}
      </div>
      <div class="properties-section">
        <div class="panel-title">消费</div>
        ${group.consumerIds.length && group.subscribedTopicIds.length ? groupActionsHtml(group, lag.rows) : `<div class="notice">至少连接一个 Topic 和一个 Consumer，Consumer Group 才能消费。</div>`}
      </div>
    `;
    wireGroupActions(group);
  }

  function lagTableHtml(rows) {
    if (!rows.length) return `<p class="hint">还没有分配任何 Topic Partition。</p>`;
    return `
      <table class="mini-table">
        <thead><tr><th>Partition</th><th>成员</th><th>位置</th><th>已提交</th><th>末端</th><th>Lag</th></tr></thead>
        <tbody>
          ${rows.map((row) => `
            <tr>
              <td>${escapeHtml(row.topicName)}[${row.partitionId}]</td>
              <td>${escapeHtml(row.assignedConsumerName)}</td>
              <td>${row.position}</td>
              <td>${row.committed}</td>
              <td>${row.end}</td>
              <td>${row.committedLag}</td>
            </tr>
          `).join("")}
        </tbody>
      </table>
    `;
  }

  function groupActionsHtml(group, rows) {
    return `
      <div class="form-row">
        <div class="field">
          <label for="pollMember">Consumer</label>
          <select id="pollMember">
            <option value="">任意已分配 Consumer</option>
            ${group.consumerIds.map((consumerId) => `<option value="${consumerId}">${escapeHtml(state.consumers[consumerId].name)}</option>`).join("")}
          </select>
        </div>
        <div class="field">
          <label for="pollBatchSize">批量条数</label>
          <input id="pollBatchSize" type="number" min="1" max="100" value="1">
        </div>
      </div>
      <div class="button-row">
        <button id="pollOneBtn" class="primary" type="button">拉取 1 条</button>
        <button id="pollBatchBtn" type="button">批量拉取</button>
        <button id="commitBtn" type="button">提交 Offset</button>
      </div>
      <div class="form-row">
        <div class="field">
          <label for="seekPartition">Partition</label>
          <select id="seekPartition">
            ${rows.map((row) => `<option value="${row.topicId}:${row.partitionId}">${escapeHtml(row.topicName)}[${row.partitionId}]</option>`).join("")}
          </select>
        </div>
        <div class="field">
          <label for="seekOffset">Offset</label>
          <input id="seekOffset" type="number" min="0" value="0">
        </div>
      </div>
      <div class="button-row">
        <button id="seekBtn" type="button">跳转 Offset</button>
        <button id="resetEarliestBtn" type="button">回到最早</button>
        <button id="resetLatestBtn" type="button">跳到最新</button>
      </div>
      <p class="hint">这些操作只改变 Consumer Group 的读取位置，不会修改或删除 Topic 中的消息。</p>
    `;
  }

  function wireGroupActions(group) {
    const pollOneButton = document.getElementById("pollOneBtn");
    if (pollOneButton) {
      pollOneButton.addEventListener("click", () => {
        const polled = pollGroup(state, group.id, 1, document.getElementById("pollMember").value || null, true);
        render();
        animateConsumedRecords(polled, group.id);
      });
    }

    const pollBatchButton = document.getElementById("pollBatchBtn");
    if (pollBatchButton) {
      pollBatchButton.addEventListener("click", () => {
        const polled = pollGroup(
          state,
          group.id,
          document.getElementById("pollBatchSize").value,
          document.getElementById("pollMember").value || null,
          true
        );
        render();
        animateConsumedRecords(polled, group.id);
      });
    }

    const commitButton = document.getElementById("commitBtn");
    if (commitButton) {
      commitButton.addEventListener("click", () => {
        commitGroup(state, group.id, true);
        render();
        animateCommitFlow(group.id);
      });
    }

    const seekButton = document.getElementById("seekBtn");
    if (seekButton) {
      seekButton.addEventListener("click", () => {
        const value = document.getElementById("seekPartition").value;
        if (!value) return;
        const parsed = parseTopicPartitionKey(value);
        seekGroup(state, group.id, parsed.topicId, parsed.partitionId, document.getElementById("seekOffset").value, true);
        render();
      });
    }

    const resetEarliestButton = document.getElementById("resetEarliestBtn");
    if (resetEarliestButton) {
      resetEarliestButton.addEventListener("click", () => {
        resetGroup(state, group.id, "earliest", true);
        render();
      });
    }

    const resetLatestButton = document.getElementById("resetLatestBtn");
    if (resetLatestButton) {
      resetLatestButton.addEventListener("click", () => {
        resetGroup(state, group.id, "latest", true);
        render();
      });
    }
  }

  function renderConsumerPanel(panel, node) {
    const consumer = state.consumers[node.id];
    const rows = assignedRowsForConsumer(state, node.id);
    panel.innerHTML = `
      ${commonNodeHtml(node)}
      <div class="properties-section">
        <div class="panel-title">Partition 分配</div>
        ${consumer.groupId ? `<p class="hint">属于 Consumer Group：${escapeHtml(getEntityName(state, consumer.groupId))}。</p>` : `<div class="notice">把 Consumer Group 连接到这个 Consumer 后，Kafka 会把 Partition 分配给 Consumer Group 成员。</div>`}
        ${rows.length ? `
          <table class="mini-table">
            <thead><tr><th>Partition</th><th>当前位置</th><th>已提交</th><th>末端</th></tr></thead>
            <tbody>
              ${rows.map((row) => `
                <tr>
                  <td>${escapeHtml(row.topicName)}[${row.partitionId}]</td>
                  <td>${row.position}</td>
                  <td>${row.committed}</td>
                  <td>${row.end}</td>
                </tr>
              `).join("")}
            </tbody>
          </table>
          <div class="button-row">
            <button id="consumerPollBtn" class="primary" type="button">让这个 Consumer 拉取 1 条</button>
          </div>
        ` : ""}
        ${consumer.lastRecord ? `<p class="hint">最近消息：${escapeHtml(recordDataText(consumer.lastRecord))}</p>` : ""}
      </div>
    `;
    const pollButton = document.getElementById("consumerPollBtn");
    if (pollButton) {
      pollButton.addEventListener("click", () => {
        const polled = pollGroup(state, consumer.groupId, 1, consumer.id, true);
        render();
        animateConsumedRecords(polled, consumer.groupId);
      });
    }
  }

  function renderBrokerPanel(panel, node) {
    const topicRows = Object.values(state.topics).flatMap((topic) => {
      return topic.partitions.map((partition) => ({
        topicName: topic.name,
        partitionId: partition.id,
        records: partition.records.length,
        end: partition.nextOffset
      }));
    });
    panel.innerHTML = `
      ${commonNodeHtml(node)}
      <div class="properties-section">
        <div class="panel-title">集群视图</div>
        <div class="notice">这里把 Broker 做了简化，只展示模拟集群里保存了哪些 Topic Partition 和消息数量。</div>
        ${topicRows.length ? `
          <table class="mini-table">
            <thead><tr><th>Partition</th><th>消息数</th><th>末端 Offset</th></tr></thead>
            <tbody>
              ${topicRows.map((row) => `
                <tr><td>${escapeHtml(row.topicName)}[${row.partitionId}]</td><td>${row.records}</td><td>${row.end}</td></tr>
              `).join("")}
            </tbody>
          </table>
        ` : `<p class="hint">还没有创建 Topic。</p>`}
      </div>
    `;
  }

  function renderLog() {
    const log = document.getElementById("eventLog");
    const events = state.events.slice().reverse();
    log.innerHTML = events.length ? events.map((event) => `
      <div class="event">
        <span class="event-time">${formatTime(event.timestamp)}</span>
        <span class="event-type">${escapeHtml(eventTypeLabel(event.type))}</span>
        <span>${escapeHtml(event.message)}</span>
      </div>
    `).join("") : `<div class="empty-state">暂无事件。</div>`;
  }

  function boot() {
    populateScenarios();
    wireGlobalActions();
    state = basicScenario();
    const selfCheck = runSelfCheck();
    document.body.dataset.selfCheckOk = String(selfCheck.ok);
    document.body.dataset.selfCheck = JSON.stringify(selfCheck);
    render();
  }

  function populateScenarios() {
    const select = document.getElementById("scenarioSelect");
    select.innerHTML = `
      <option value="basic">基础：生产与消费</option>
      <option value="keyed">Key Partition 与顺序</option>
      <option value="group">同组多个 Consumer</option>
      <option value="independent">两个 Consumer Group 独立读取</option>
      <option value="lag">Lag 与手动提交</option>
      <option value="replay">跳转 Offset 与回放</option>
    `;
  }

  function wireGlobalActions() {
    document.querySelectorAll(".tool-item").forEach((item) => {
      item.addEventListener("click", () => addNode(item.dataset.type));
      item.addEventListener("dragstart", (event) => {
        event.dataTransfer.setData("text/plain", item.dataset.type);
      });
    });

    const canvas = document.getElementById("canvas");
    canvas.addEventListener("dragover", (event) => {
      event.preventDefault();
      canvas.classList.add("drag-over");
    });
    canvas.addEventListener("dragleave", () => canvas.classList.remove("drag-over"));
    canvas.addEventListener("drop", (event) => {
      event.preventDefault();
      canvas.classList.remove("drag-over");
      const type = event.dataTransfer.getData("text/plain");
      if (!TYPE_LABELS[type]) return;
      const point = canvasPointFromEvent(event);
      addNode(type, point.x / canvasZoom - NODE_BASE_WIDTH / 2, point.y / canvasZoom - NODE_BASE_HEIGHT / 2);
    });
    document.getElementById("zoomOutBtn")?.addEventListener("click", () => setCanvasZoom(canvasZoom - CANVAS_ZOOM_STEP));
    document.getElementById("zoomInBtn")?.addEventListener("click", () => setCanvasZoom(canvasZoom + CANVAS_ZOOM_STEP));
    document.getElementById("zoomResetBtn")?.addEventListener("click", () => setCanvasZoom(1));

    document.getElementById("loadScenarioBtn").addEventListener("click", () => {
      const value = document.getElementById("scenarioSelect").value;
      state = scenarioFactories[value]();
      clearFlowLayer();
      render();
    });
    document.getElementById("saveBtn").addEventListener("click", () => saveState(true));
    document.getElementById("loadSavedBtn").addEventListener("click", loadSavedState);
    document.getElementById("exportBtn").addEventListener("click", () => openJsonModal("export"));
    document.getElementById("importBtn").addEventListener("click", () => openJsonModal("import"));
    document.getElementById("resetBtn").addEventListener("click", () => {
      state = createEmptyState();
      clearFlowLayer();
      logEvent(state, "reset", "工作台已重置。");
      render();
    });
    document.getElementById("clearLogBtn").addEventListener("click", () => {
      state.events = [];
      render();
    });
    document.getElementById("closeModalBtn").addEventListener("click", closeJsonModal);
    document.getElementById("applyJsonBtn").addEventListener("click", applyJsonImport);
    document.getElementById("copyJsonBtn").addEventListener("click", copyJson);
    document.getElementById("jsonModal").addEventListener("click", (event) => {
      if (event.target.id === "jsonModal") closeJsonModal();
    });
  }

  function setTopicPartitions(draft, topicId, count) {
    const topic = draft.topics[topicId];
    topic.partitionCount = count;
    topic.partitions = Array.from({ length: count }, (_, index) => createPartition(index));
  }

  function createBaseTopology() {
    const draft = createEmptyState();
    const broker = createNode(draft, "broker", 28, 30, "本地 Broker");
    const producer = createNode(draft, "producer", 260, 48, "订单 Producer");
    const topic = createNode(draft, "topic", 500, 48, "订单 Topic");
    const group = createNode(draft, "group", 500, 260, "计费 Consumer Group");
    const consumer = createNode(draft, "consumer", 260, 260, "计费 Consumer-1");
    draft.selectedId = producer;
    setTopicPartitions(draft, topic, 3);
    connectNodes(draft, producer, topic, false);
    connectNodes(draft, topic, group, false);
    connectNodes(draft, group, consumer, false);
    logEvent(draft, "load", "学习场景已加载。");
    return { draft, broker, producer, topic, group, consumer };
  }

  function basicScenario() {
    const { draft } = createBaseTopology();
    logEvent(draft, "hint", "Producer 表单已预填。点击“发送消息”，再选择 Consumer Group 并点击“拉取 1 条”。");
    return draft;
  }

  function keyedOrderingScenario() {
    const { draft, producer, topic } = createBaseTopology();
    draft.producers[producer].strategy = "key-hash";
    ["用户A", "用户B", "用户A", "用户C", "用户A"].forEach((key, index) => {
      produceRecord(draft, { producerId: producer, topicId: topic, key, value: `事件-${index + 1}` }, true);
    });
    draft.selectedId = topic;
    return draft;
  }

  function multiConsumerScenario() {
    const { draft, producer, topic, group } = createBaseTopology();
    const c2 = createNode(draft, "consumer", 738, 260, "计费 Consumer-2");
    connectNodes(draft, group, c2, true);
    ["a", "b", "c", "d", "e", "f"].forEach((key) => {
      produceRecord(draft, { producerId: producer, topicId: topic, key, value: `订单-${key}` }, true);
    });
    draft.selectedId = group;
    return draft;
  }

  function independentGroupsScenario() {
    const { draft, producer, topic, group } = createBaseTopology();
    draft.groups[group].name = "计费 Consumer Group";
    const analyticsGroup = createNode(draft, "group", 738, 92, "分析 Consumer Group");
    const analyticsConsumer = createNode(draft, "consumer", 738, 260, "分析 Consumer-1");
    connectNodes(draft, topic, analyticsGroup, true);
    connectNodes(draft, analyticsGroup, analyticsConsumer, true);
    ["订单已创建", "订单已支付", "订单已发货"].forEach((value) => {
      produceRecord(draft, { producerId: producer, topicId: topic, key: "订单-1001", value }, true);
    });
    pollGroup(draft, group, 1, null, true);
    draft.selectedId = analyticsGroup;
    return draft;
  }

  function lagScenario() {
    const { draft, producer, topic, group } = createBaseTopology();
    ["消息一", "消息二", "消息三", "消息四", "消息五"].forEach((value) => {
      produceRecord(draft, { producerId: producer, topicId: topic, key: "", value }, true);
    });
    pollGroup(draft, group, 2, null, true);
    draft.selectedId = group;
    return draft;
  }

  function replayScenario() {
    const { draft, producer, topic, group } = createBaseTopology();
    ["登录", "浏览商品", "下单结算"].forEach((value) => {
      produceRecord(draft, { producerId: producer, topicId: topic, key: "用户-7", value }, true);
    });
    pollGroup(draft, group, 3, null, true);
    commitGroup(draft, group, true);
    seekGroup(draft, group, topic, hashString("用户-7") % draft.topics[topic].partitions.length, 1, true);
    draft.selectedId = group;
    return draft;
  }

  function runSelfCheck() {
    const draft = createEmptyState();
    const producer = createNode(draft, "producer", 40, 40, "p");
    const topic = createNode(draft, "topic", 260, 40, "t");
    const group = createNode(draft, "group", 260, 240, "g");
    const c1 = createNode(draft, "consumer", 40, 240, "c1");
    const c2 = createNode(draft, "consumer", 480, 240, "c2");
    setTopicPartitions(draft, topic, 3);
    connectNodes(draft, producer, topic, true);
    connectNodes(draft, topic, group, true);
    connectNodes(draft, group, c1, true);
    connectNodes(draft, group, c2, true);
    produceRecord(draft, { producerId: producer, topicId: topic, key: "same", value: "a" }, true);
    produceRecord(draft, { producerId: producer, topicId: topic, key: "same", value: "b" }, true);
    produceRecord(draft, { producerId: producer, topicId: topic, key: "other", value: "c" }, true);
    const samePartition = hashString("same") % 3;
    const sameRecords = draft.topics[topic].partitions[samePartition].records.filter((record) => record.key === "same");
    const polled = pollGroup(draft, group, 2, null, true);
    commitGroup(draft, group, true);
    seekGroup(draft, group, topic, samePartition, 0, true);
    const lag = groupLag(draft, group);

    const assertions = [
      draft.connections.length === 4,
      draft.groups[group].consumerIds.length === 2,
      sameRecords.length === 2,
      sameRecords[0].offset < sameRecords[1].offset,
      polled.length === 2,
      Object.keys(draft.groups[group].committed).length === 3,
      lag.rows.some((row) => row.topicId === topic && row.partitionId === samePartition && row.position === 0)
    ];

    return {
      ok: assertions.every(Boolean),
      assertions,
      records: draft.topics[topic].partitions.reduce((sum, partition) => sum + partition.records.length, 0),
      committedLag: lag.totalCommittedLag,
      events: draft.events.length
    };
  }

  if (typeof window !== "undefined") {
    window.KafkaSimulatorTest = {
      runSelfCheck,
      getState: () => JSON.parse(JSON.stringify(state)),
      loadScenario: (name) => {
        state = scenarioFactories[name] ? scenarioFactories[name]() : basicScenario();
        render();
        return state;
      }
    };
  }

  if (typeof document !== "undefined") {
    document.addEventListener("DOMContentLoaded", boot);
  }
})();
