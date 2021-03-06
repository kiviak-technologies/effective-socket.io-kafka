
/**
 * Dependencies
 */

const Adapter = require('socket.io-adapter');
const kafka = require('kafka-node');
const uuidv4 = require('uuid/v4');

/**
 * Constants
 */

const BROADCAST = 0;
const ADDALL = 1;
const DEL = 2;
const DELALL = 3;

/**
 * Returns a KafkaAdapter class
 */
const factory = (options) => class KafkaAdapter extends Adapter {

  constructor(nsp) {
    super(nsp);
    this.options = options || {};
    this.options.groupId = this.uuid = uuidv4();
    this.options.topic = this.topic = this.options.topic || 'socketio';
    this.kafkaClient = new kafka.KafkaClient(options);
    this.kafkaProducer = new kafka.Producer(this.kafkaClient, options);
    this.kafkaConsumer = new kafka.Consumer(this.kafkaClient, [], options);
    this.kafkaConsumer.on('message', this.onMessage.bind(this));
    this.kafkaConsumer.addTopics([this.topic], (err) => console.log(err));
  }

  onMessage(message) {
    const msg = JSON.parse(message.value);
    if (msg.uuid == this.uuid) {
      return;
    }
    switch (msg.type) {
      case BROADCAST:
        if (msg.packet.nsp != this.nsp.name) {
          break;
        }
        super.broadcast(msg.packet, msg.opts);
        break;
      case ADDALL:
        if (!this.nsp.connected[msg.id]) {
          break;
        }
        super.addAll(msg.id, msg.rooms);
        break;
      case DEL:
        super.del(msg.id, msg.room);
        break;
      case DELALL:
        super.delAll(msg.id);
        break;
    }
  }

  /**
   * @override
   */
  broadcast(packet, opts) {
    super.broadcast(packet, opts);
    this.kafkaProducer.send([{
      topic: this.topic,
      messages: JSON.stringify({ uuid: this.uuid, type: BROADCAST, packet, opts })
    }], (err, data) => console.log(err, data));
  }

  /**
    * @override
    * @todo ACK, async fn call
    * -> PING for n = (number of nodes)-1.
    * -> Send random id with request.
    * -> Wait (with timeout) for n responses identified by id+1.
    * -> call fn.
    */
  addAll(id, rooms, fn) {
    if (this.nsp.connected[id]) {
      return super.addAll(id, rooms, fn);
    }
    this.kafkaProducer.send([{
      topic: this.topic,
      messages: JSON.stringify({ uuid: this.uuid, type: ADDALL, id, rooms })
    }], (err) => fn && fn(err));
  }

  /**
   * @override
   */
  add(id, room, fn) {
    return this.addAll(id, [room], fn);
  }

  /**
   * @override
   * @todo cf addAll
   */
  delAll(id, fn) {
    super.delAll(id);
    this.kafkaProducer.send([{
      topic: this.topic,
      messages: JSON.stringify({ uuid: this.uuid, type: DELALL, id })
    }], (err) => fn && fn(err));
  }

  /**
   * @override
   * @todo cf addAll
   */
  del(id, room, fn) {
    super.del(id, room);
    this.kafkaProducer.send([{
      topic: this.topic,
      messages: JSON.stringify({ uuid: this.uuid, type: DEL, id, room })
    }], (err) => fn && fn(err));
  }

};

/**
 * Exports
 */

module.exports = factory;