//  This is a Google Cloud Function that processes the callback from Sensit for the magnet mode.
//  The code here handles 2 main functions:
//  (1) Callbacks from Sensit when status has changed
//  (2) Keeping the Slack Real Time Messaging connection alive so that the bot status shows green (now moved to unago)

//  Parameters for Slack.
const token = '(removed)';
const broadcastChannel = '(removed)';
const topicName = 'waitForToilet';
const toiletOpenMessage = '*** T O I L E T  I S  O P E N  ! ! ! ***';
const toiletCloseMessage = 'toilet closed';

//  Parameters for calling AWS.
const awsSigfoxCallback = 'https://91r65moej0.execute-api.ap-southeast-1.amazonaws.com/prod/sigfoxCallback?comment=Sensit_Callback';  //  The callback URL
const method = 'POST';
const credentials = null;
const headers = {
  Accept: 'application/json',
  'Content-Type': 'application/json',
};

const fetch = require('node-fetch');
const uuidModule = require('uuid');
const WebSocket = require('ws');

function composeReponse(body) {
  //  Body contains the POST body of a sensit callback (see below for examples)
  //  Return a basic response object that we will send to AWS, without sensor values.
  if (!body) return null;  //  Skip if no body.
  const device = body.serial_number;
  if (!device) return null;  //  Skip if no device ID.
  //  Number of milliseconds since 1 Jan 1970 GMT e.g. 1517193184063
  const timestamp = Date.now();
  const datetime = new Date(timestamp)
    .toISOString().replace('T', ' ')
    .substr(0, 19); //  datetime contains UTC time in text.
  const localdatetime = new Date(timestamp + (8 * 60 * 60 * 1000))
    .toISOString().replace('T', ' ')
    .substr(0, 19); //  localdatetime contains localtime in text.
  const uuid = uuidModule.v4();  //  Assign a UUID for message tracking.
  //  These are the minimum fields needed.
  const response = { uuid, device, timestamp, datetime, localdatetime };
  return response;
}

function flattenObject(obj, prefix) {
  /* obj contains {
      "date": "2018-01-29T01:55Z",
      "signal_level": "good",
      "data": "1:1" }
     If prefix='magnet_', then return
      "magnet_date": "2018-01-29T01:55Z",
   	  "magnet_signal_level": "good",
      "magnet_data": "0:1" */
  const result = {};
  Object.keys(obj).forEach(key => {
    const val = obj[key];
    //  We only copy scalar values: strings, numbers.  Don't copy arrays and objects.
    if (typeof val === 'string' || typeof val === 'number') {
      result[prefix + key] = val;
    }
  });
  return result;
}

function flattenSensor(sensor) {
  /* sensor contains: {
      "id": "29270",
      "history": [
        {
          "date": "2018-01-29T01:55Z",
          "signal_level": "good",
          "data": "1:1"
        }
      ],
      "sensor_type": "magnet",
      "config": {
        "threshold": 0
      }    }
  Flattern the object and return: {
	magnet_id: "29270",
    magnet_config_threshold: 0,
    "magnet_date": "2018-01-29T01:55Z",
    "magnet_signal_level": "good",
    "magnet_data": "1:1" }  */
  const sensor_type = sensor.sensor_type;
  const result = {};
  if (sensor.id) result[`${sensor_type}_id`] = sensor.id;  //  e.g. magnet_id: "29270",
  if (sensor.history && sensor.history[0]) {
    // Convert the first history entry {
    //   "date": "2018-01-29T01:55Z",
    //   "signal_level": "good",
    //   "data": "1:1" }
    // into: {
    //   "magnet_date": "2018-01-29T01:55Z",
    //   "magnet_signal_level": "good",
    //   "magnet_data": "0:1",
    //   "magnet_status": 0,
    //   "magnet_updates": 1 }

    //  Make a copy of the entry before we update it.
    const entry = Object.assign({}, sensor.history[0]);
    if (sensor_type === 'magnet' && entry.data) {
      //  Only for magnet: split
      //    data": "0:1",
      //   into status: 0, updates: 1
      const dataSplit = entry.data.split(':');
      entry.status = parseInt(dataSplit[0], 10);
      if (dataSplit.length >= 2) {
        entry.updates = parseInt(dataSplit[1], 10);
      }
    }
    const updatedEntry = flattenObject(entry, `${sensor_type}_`);
    //  Copy the flattened entry into the result.
    Object.assign(result, updatedEntry);
  }
  return result;
  //  TODO: magnet_config_threshold: 0,
}

function processMessage(body) {
  //  Body contains the POST body of a sensit callback (see below for examples)
  //  Return a response object that we will send to AWS.
  if (!body) return null;  //  Skip if no body.
  const response = composeReponse(body);
  //  Get all the strings and numbers at the root level.  We will handle objects later.
  const root = flattenObject(body, '');
  //  Copy the root level strings and numbers into the response.
  Object.assign(response, root);
  if (body.sensors) {
    //  Copy each sensor value, after flattening.
    body.sensors.forEach(sensor => {
      const sensorValues = flattenSensor(sensor);
      Object.assign(response, sensorValues);
    });
  }
  return response;
}

function processWeb(req, res) {
  //  Web callback from Sensit: Process the message into a flat JSON object than can be processed by AWS.
  let doorIsOpen = false;
  const text = JSON.stringify(req.body);
  const response = processMessage(req.body);
  response.text = text;
  const body = JSON.stringify(response, null, 2);
  console.log(JSON.stringify({ body: req.body, response }, null, 2));
  //  Enqueue a task in unago/task to change bot status to green: https://task-dot-unabiz-unago.appspot.com/enqueue?device=abc&magnet_date=aaa&magnet_status=123
  const url = `https://task-dot-unabiz-unago.appspot.com/enqueue?device=${response.device}&magnet_date=${response.magnet_date}&magnet_status=${response.magnet_status}`;
  return fetch(url, { method: 'GET', headers })
    .then(result => { console.log(JSON.stringify({ bot: result }, null, 2)); return result; })
    //  Send to AWS.
    .then(() => fetch(awsSigfoxCallback, { method, credentials, headers, body }))
    .then(result => { console.log(JSON.stringify({ aws: result }, null, 2)); return result; })
    //  Call to AWS succeeded. Tell everyone in the waiting_for_relief chat room of the new status.
    .then(() => {
      doorIsOpen = (response.magnet_status === 0);
      //  This broadcast will cause some waiting processes to quit.
      return broadcastSlackMessage(
        doorIsOpen
          ? toiletOpenMessage
          : toiletCloseMessage);
    })
    .then(() => {
      //  If door is open, start another process to keep the status green.
      if (doorIsOpen) return sendToSelf();
      //  Else just quit.
      return 'OK';
    })
    .then(() => res.status(200).send('"OK"'))
    .catch(err => {
      //  Call to AWS failed, dump the error.  But don't return error to Sensit.
      console.error(err.message, err.stack);
      return res.status(200).send('"Error"');
    });
}

function processQueue(event) {
  // We received a queue message to keep alive.  We connect to Slack to keep the status green.
  const pubsubMessage = event.data;
  // console.log(Buffer.from(pubsubMessage.data, 'base64').toString());
  return setActivePresence()
    .then(result => {
      console.log({ result });
      return result;
    })
    //  Suppress the error so Google wont restart.
    .catch(err => {
      console.error(err.message, err.stack);
      return err;
    });
}

//  There are 2 startup functions here, depending whether we are running web or message queue mode.
exports.main =
  (process.env.FUNCTION_TRIGGER_TYPE === 'CLOUD_PUBSUB_TRIGGER')
    ? processQueue
    : processWeb;

function connectToSlackRTM() {
  //  Connect to the Slack Real Time Message Bot API.  Returns a websocket URL.
  const url = 'https://slack.com/api/rtm.connect?token=' + token + '&pretty=1';
  return fetch(url, { method: 'GET', headers })
    .then(res => res.json())
    .then(result => {
      //  Connection to Slack succeeded.
      console.log({ result });
      return result;
    })
    .catch(err => {
      //  Call to Slack failed, dump the error.
      console.error(err.message, err.stack);
      throw err;
    });
}

function connectToWebSocket(url) {
  if (!url) throw new Error('missing url');
  return new Promise((resolve, reject) => {
    //  Connect to the websocket URL.
    let timeout = null;
    const ws = new WebSocket(url);

    ws.on('open', function open() {
      ws.send('something');
    });

    ws.on('message', function incoming(data) {
      console.log(data);
      if (data.text && data.text.indexOf(toiletCloseMessage) >= 0) {
        //  Toilet is closed, quit.
        //  Cancel the timer.
        clearTimeout(timeout);
        timeout = null;
        console.log('***Quitting now');
        ws.close();
        setTimeout(() => process.exit(0), 5 * 1000);  //  Terminate after a while.
        return resolve('OK');
      }
    });

    console.log('Waiting for toilet closed...');
    // keepAlive();  //  Every 20 to 40 seconds, send a message to trigger itself and stay alive.
    timeout = setTimeout(() => {
      //  Delay a long time before timeout.
      console.log('Timeout');
      sendToSelf();
      ws.close();
      return resolve('timeout');  //  Quit.
    }, Math.min(500 * 1000, 400 * 1000 * (Math.random() + 1)));  //  Delay 400 to 500 secs.
  });
}

function setActivePresence() {
  //  Connect to Slack Real Time Messaging and keep alive so status shows green.
  let url = null;
  return connectToSlackRTM()
    .then(res => { url = res.url; if (!url) throw new Error('missing websocket URL'); })
    .then(() => connectToWebSocket(url))
    .catch(err => {
      //  Call to Slack failed, dump the error.
      console.error(err.message, err.stack);
      throw err;
    });
}

function broadcastSlackMessage(msg) {
  //  Broadcast the text Slack message to the "waiting_for_relief" channel.
  const cmd = { text: msg };
  return fetch(broadcastChannel, { method: 'POST', headers, body: JSON.stringify(cmd) })
    //.then(res => res.json())
    .then(result => {
      //  Connection to Slack succeeded.
      console.log({ result });
      return result;
    })
    .catch(err => {
      //  Call to Slack failed, dump the error.
      console.error(err.message, err.stack);
      throw err;
    });
}

function sendToSelf() {
  //  Send a message to myself.  So another instance will start running and wait for toilet.
  const obj = { status: 'waiting' };
  const buf = new Buffer(JSON.stringify(obj));
  const topic = require('@google-cloud/pubsub')().topic(topicName);
  return topic.publisher().publish(buf)
    .then(result => {
      console.log({ result });
      return result;
    })
    .catch(err => {
      console.error(err.message, err.stack);
      throw err;
    });
}

function keepAlive() {
  //  Keep alive by sending messages to itself every 200 to 400 seconds.
  const delay = 200 * 1000 * (Math.random() + 1);
  setTimeout(() => {
    sendToSelf();
    // keepAlive();
  }, delay);
}

/*
When you close the door, sensit sends:
{
  "mode": 5,
  "sensors": [
    {
      "id": "29266",
      "history": [
        {
          "date": "2018-01-29T01:55Z",
          "signal_level": "good",
          "data": "25.4"
        }
      ],
      "sensor_type": "temperature_humidity",
      "config": {
        "period": 0,
        "threshold_up": 0,
        "threshold_down": 0
      }
    },
    {
      "id": "29270",
      "history": [
        {
          "date": "2018-01-29T01:55Z",
          "signal_level": "good",
          "data": "1:1"
        }
      ],
      "sensor_type": "magnet",
      "config": {
        "threshold": 0
      }
    }
  ],
  "device_model": "",
  "activation_date": "2016-10-20T08:08Z",
  "last_comm_date": "2018-01-29T01:55Z",
  "serial_number": "1CB074",
  "id": "7695",
  "battery": 60,
  "last_config_date": "0002-11-29T23:00Z"
}
*/

/*
When you open the door, sensit sends:
{
  "mode": 5,
  "sensors": [
    {
      "id": "29266",
      "history": [
        {
          "date": "2018-01-29T01:56Z",
          "signal_level": "average",
          "data": "25.2"
        }
      ],
      "sensor_type": "temperature_humidity",
      "config": {
        "period": 0,
        "threshold_up": 0,
        "threshold_down": 0
      }
    },
    {
      "id": "29270",
      "history": [
        {
          "date": "2018-01-29T01:56Z",
          "signal_level": "average",
          "data": "0:1"
        }
      ],
      "sensor_type": "magnet",
      "config": {
        "threshold": 0
      }
    }
  ],
  "device_model": "",
  "activation_date": "2016-10-20T08:08Z",
  "last_comm_date": "2018-01-29T01:56Z",
  "serial_number": "1CB074",
  "id": "7695",
  "battery": 60,
  "last_config_date": "0002-11-29T23:00Z"
}
*/

/*
// Example input: {"message": "Hello!"}
if (req.body.message === undefined) {
  // This is an error case, as "message" is required.
  res.status(400).send('No message defined!');
} else {
  // Everything is okay.
  console.log(req.body.message);
  res.status(200).send('Success: ' + req.body.message);
}
*/

