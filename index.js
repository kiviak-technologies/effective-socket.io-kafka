
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
    this.kafkaConsumer.addTopics([this.topic]);
  }

  onMessage(message) {
    const msg = JSON.parse(message.value);
    if (msg.uuid == this.uuid) {
      return;
    }
    switch (msg.type) {
      case BROADCAST:
        if (packet.nsp != this.nsp.name) {
          break;
        }
        super.broadcast(msg.packet, msg.opts);
        break;
    }
  }

  broadcast(packet, opts) {
    super.broadcast(packet, opts);
    this.kafkaProducer.send([{
      topic: this.topic,
      messages: JSON.stringify({ uuid: this.uuid, type: BROADCAST, packet, opts })
    }]);
  }

};

/**
 * Exports
 */

module.exports = factory;