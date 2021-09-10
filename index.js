// Warning: All of the code in this file sucks.
// I was excited and in a hurry.
// I'm sorry.

const MongoClient = require('mongodb').MongoClient;
const express = require('express');
const morgan = require('morgan');
const app = express();
const fetch = require('node-fetch');
const { Parser } = require('json2csv');
const { PrometheusDriver } = require('prometheus-query');
const port = 8080;
const itemNames = {}; let offerscsv; let requestscsv; let events;

let offers = [];
let requests = [];

const languages = {
  English: 'en',
  Pусский: 'ru',
  Deutsch: 'de',
  Español: 'es',
  Français: 'fr',
  Italiano: 'it',
  日本: 'ja',
  한국인: 'ko',
  Português: 'pt',
  中国人: 'zh'
};
const languagesProm = {
  English: 'English',
  Pусский: 'Russian',
  Deutsch: 'German',
  Español: 'Spanish',
  Français: 'French',
  Italiano: 'Italian',
  日本: 'Japanese',
  한국인: 'Korean',
  Português: 'Portugese',
  中国人: 'Chinese'
};

const languagesPromShort = {
  English: 'en',
  Russian: 'ru',
  German: 'de',
  Spanish: 'es',
  French: 'fr',
  Italian: 'it',
  Japanese: 'ja',
  Korean: 'ko',
  Portugese: 'pt',
  Chinese: 'zh'
};

const config = {
  mongoUrl: process.env.mongo_uri,
  mongoDB: process.env.mongo_db,
  prometheusUrl: process.env.prom_uri,
  grafanaBearerToken: process.env.grafana_token
};

const prom = new PrometheusDriver({
  endpoint: config.prometheusUrl,
  timeout: 60000
});

app.use(morgan('combined'));

app.get('/offers.csv*', (req, res) => {
  res.send(offerscsv);
});

app.get('/requests.csv*', (req, res) => {
  res.send(requestscsv);
});

app.get('/offers*', (req, res) => {
  res.send(offers);
});

app.get('/requests*', (req, res) => {
  res.send(requests);
});

app.get('/metrics*', async (req, res) => {
  res.set('Content-Type', register.contentType);
  res.send(await register.metrics());
});

app.get('/events*', (req, res) => {
  res.send(events);
});

app.get('/', (req, res) => {
  res.send(`<h1>Makeshift's Shop Titans Market Data API</h1>
  <p>This API is owned and maintained by Makeshift#7058 (Discord)</p>
  <p>The market information is scraped by manually hitting the games websocket using an authenticated account. I have been asked by Kabam not to disclose the method for doing this, so please don't ask.</p>
  <p>The data returned by the market does <i>not</i> contain every possible offer or request. It only contains the lowest offer and highest request.
  <p>Due to rate limiting, I can only update a single item every 2.5 seconds or so. Consequently, it takes around an hour for all items prices to be updated.</p>
  <p>This API caches data for the last 10 minutes.</p>
  <p>Here are the available endpoints and their descriptions:<br>
  <ul>
    <li><a href="/offers">/offers</a> - A JSON array of all 'active' (at least in the last hour) market offers on a per-item/per-rarity basis, as provided by the game API.<br>
    The keys of each object in the array are as follows:
    <p><ul>
    <li><span style="font-family:'Lucida Console', monospace">_id:</span> an arbitrary ID from MongoDB</li>
    <li><span style="font-family:'Lucida Console', monospace">created:</span> The unix time (in ms) that the trade was created according to the game API</li>
    <li><span style="font-family:'Lucida Console', monospace">active:</span> Whether the trade was active last time we checked (should always be true)</li>
    <li><span style="font-family:'Lucida Console', monospace">gemsPrice:</span> The lowest available price for the item in gems</li>
    <li><span style="font-family:'Lucida Console', monospace">goldPrice:</span> The lowest available price for the item in gold</li>
    <li><span style="font-family:'Lucida Console', monospace">avgGemsPrice:</span> The average historical gem price for this item over all collected data</li>
    <li><span style="font-family:'Lucida Console', monospace">avgGoldPrice:</span> The average historical gold price for this item over all collected data</li>
    <li><span style="font-family:'Lucida Console', monospace">itemsAtGemPrice:</span> The number of items available at the lowest gem price (I think? This may be <i>all</i> available items for sale for gems, I'm not sure)</li>
    <li><span style="font-family:'Lucida Console', monospace">itemsAtGoldPrice:</span> The number of items available at the lowest gold price (I think? This may be <i>all</i> available items for sale for gold, I'm not sure)</li>
    <li><span style="font-family:'Lucida Console', monospace">lastSeen:</span> The last unix time (in ms) that we updated the price for this item</li>
    <li><span style="font-family:'Lucida Console', monospace">tags:</span> Tags assigned to this item - usually its rarity, and used to contain any enchantments back when they were able to be sold on the market</li>
    <li><span style="font-family:'Lucida Console', monospace">tier:</span> The tier of the item</li>
    <li><span style="font-family:'Lucida Console', monospace">uid:</span> The name of the item used internally by the game</li>
    <li><span style="font-family:'Lucida Console', monospace">component:</span> true if the item is considered a component (otherwise it's a piece of equipment)</li>
    <li><span style="font-family:'Lucida Console', monospace">${Object.keys(languages).join(' | ')}</span>: The localised name for the item in the given language</li>
    </ul></p></li>
    <li><a href="/requests">/requests</a> - A JSON array of all 'active' (at least in the last hour) market requests on a per-item/per-rarity basis, as provided by the game API. Same format as <span style="font-family:'Lucida Console', monospace">/offers</span></li>
    <li><a href="/offers.csv">/offers.csv</a> - A CSV output of all 'active' (at least in the last hour) market offers on a per-item/per-rarity basis, as provided by the game API. Same format as <span style="font-family:'Lucida Console', monospace">/offers</span> but in CSV<br></li>
    <li><a href="/requests.csv">/requests.csv</a> - A CSV output of all 'active' (at least in the last hour) market requests on a per-item/per-rarity basis, as provided by the game API. Same format as <span style="font-family:'Lucida Console', monospace">/requests</span> but in CSV<br></li>
    <li><a href="/metrics">/metrics</a> - A Prometheus-compatible metrics output for the API (Note that the languages are in English due to label name limitations in Prometheus)<br></li>
    <li><a href="/events">/events</a> - An array of current and previous game events as extracted from the game news, in the raw form given by the game API<br></li>
    <li><a href="https://stprom.makeshift.ninja/">https://stprom.makeshift.ninja</a> - The Prometheus database containing historical data</li>
    <li><a href="https://stgraphs.makeshift.ninja/">https://stgraphs.makeshift.ninja/</a> - Graphs from the Prometheus database</li>
    <li><a href="https://github.com/Makeshift/shop_titans_dashboards">Makeshift/shop_titans_dashboards</a> - This is how I generate all the Grafana dashboards</li>
    <li><a href="https://github.com/Makeshift/shop-titans-market-api">Makeshift/shop-titans-market-api</a> - This is the code for this API</li>
    <li><a href="https://github.com/Makeshift/shoptitans_scraper">Makeshift/shoptitans_scraper</a> - This is the code for the scraper that collects market data. It's private because Kabam asked me to make it private, but I'm super proud of it still.</li>
  </ul>
    `);
});

app.listen(port, () => {
  console.log(`Example app listening at http://localhost:${port}`);
});

async function go () {
  await Promise.all(Object.keys(languages).map(async language => {
    itemNames[language] = await (await fetch(`https://playshoptitans.com/gameData/texts_${languages[language]}.json`)).json();
  }));
  const client = await MongoClient.connect(config.mongoUrl);
  const db = client.db(config.mongoDB);
  const offers = db.collection('offers');
  const requests = db.collection('requests');
  const events = db.collection('events');
  getLoop(offers, requests, events);
}

go();

function sleep (ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

function dataToCSV (data) {
  const simpleData = data.map(offer => {
    const languageNames = {};
    Object.keys(itemNames).forEach(language => {
      languageNames[language] = itemNames[language].texts[`${offer.uid}_name`];
    });
    return {
      uid: offer.uid,
      ...languageNames,
      tier: offer.tier,
      rarity: offer.tags[0] || 'normal',
      goldPrice: offer.goldPrice,
      itemsAtGoldPrice: offer.itemsAtGoldPrice,
      gemsPrice: offer.gemsPrice,
      itemsAtGemsPrice: offer.itemsAtGemPrice,
      avgGemsPrice: offer.avgGemsPrice,
      avgGoldPrice: offer.avgGoldPrice,
      component: typeof offer.component === 'undefined' ? 'false' : offer.component
    };
  });
  const parser = new Parser({
    fields: Object.keys(simpleData[0])
  });
  return parser.parse(simpleData);
}
const client = require('prom-client');
const Registry = client.Registry;
const register = new Registry();
const labelNames = ['uid', 'type', 'tier', 'rarity', 'unique', 'component', ...Object.values(languagesProm)];
const registeredMetrics = {
  gold_price: new client.Gauge({
    name: 'gold_price',
    help: 'Gold price',
    labelNames: labelNames
  }),
  items_at_gold_price: new client.Gauge({
    name: 'items_at_gold_price',
    help: 'Items at gold price',
    labelNames: labelNames
  }),
  gem_price: new client.Gauge({
    name: 'gem_price',
    help: 'Gem price',
    labelNames: labelNames
  }),
  items_at_gem_price: new client.Gauge({
    name: 'items_at_gem_price',
    help: 'Items at gem price',
    labelNames: labelNames
  })
};
Object.values(registeredMetrics).forEach(metric => register.registerMetric(metric));

function registerMetric (client, offer, suffix) {
  try {
    const rarity = offer.tags[0] || 'normal';
    const unique = `${offer.uid}_${rarity}_${suffix}`;
    const languageNames = {};
    Object.keys(itemNames).forEach(language => {
      languageNames[languagesProm[language]] = itemNames[language].texts[`${offer.uid}_name`];
    });
    const labels = {
      uid: offer.uid,
      rarity: rarity,
      tier: offer.tier,
      type: suffix,
      unique: unique,
      component: typeof offer.component === 'undefined' ? 'false' : offer.component,
      ...languageNames
    };

    if (offer.goldPrice > 0) {
      registeredMetrics.gold_price.labels(labels).set(offer.goldPrice);
    } else {
      registeredMetrics.gold_price.remove(labels);
    }
    if (offer.itemsAtGoldPrice > 0) {
      registeredMetrics.items_at_gold_price.labels(labels).set(offer.itemsAtGoldPrice);
    } else {
      registeredMetrics.items_at_gold_price.remove(labels);
    }
    if (offer.gemsPrice > 0) {
      registeredMetrics.gem_price.labels(labels).set(offer.gemsPrice);
    } else {
      registeredMetrics.gem_price.remove(labels);
    }
    if (offer.itemsAtGemPrice > 0) {
      registeredMetrics.items_at_gem_price.labels(labels).set(offer.itemsAtGemPrice);
    } else {
      registeredMetrics.items_at_gem_price.remove(labels);
    }
  } catch (e) {
    console.log(e);
  }
}

async function queryPrometheus () {
  console.log(getTime(), 'Querying Prometheus for avg gold prices...');
  const goldResults = await prom.instantQuery('avg_over_time(gold_price[10y])');
  const averages = {};
  goldResults.result.forEach(row => {
    if (!averages[row.metric.labels.uid]) averages[row.metric.labels.uid] = {};
    if (!averages[row.metric.labels.uid][row.metric.labels.rarity]) averages[row.metric.labels.uid][row.metric.labels.rarity] = {};
    averages[row.metric.labels.uid][row.metric.labels.rarity].avgGoldPrice = row.value.value;
  });
  console.log(getTime(), 'Querying Prometheus for avg gem prices...');
  const gemResults = await prom.instantQuery('avg_over_time(gem_price[10y])');
  gemResults.result.forEach(row => {
    if (!averages[row.metric.labels.uid]) averages[row.metric.labels.uid] = {};
    if (!averages[row.metric.labels.uid][row.metric.labels.rarity]) averages[row.metric.labels.uid][row.metric.labels.rarity] = {};
    averages[row.metric.labels.uid][row.metric.labels.rarity].avgGemsPrice = row.value.value;
  });
  return averages;
}

function addLanguages (objects) {
  return objects.map(object => {
    const languageNames = {};
    Object.keys(itemNames).forEach(language => {
      languageNames[language] = itemNames[language].texts[`${object.uid}_name`];
    });
    return {
      ...object,
      ...languageNames
    };
  });
}

function addAverages (objects, averages) {
  return objects.map(object => {
    const tag = object.tags[0] || 'normal';
    const average = averages[object.uid]?.[tag] || {};
    return {
      ...object,
      ...average
    };
  });
}

function getTime () {
  const today = new Date();
  return `${today.getFullYear()}-${today.getMonth() + 1}-${today.getDate()} ${today.getHours()}:${today.getMinutes()}:${today.getSeconds()}`;
}

async function registerEvents (events) {
  events.forEach(async event => {
    const checkIfEventExists = await (await fetch(`https://${process.env.grafana_uri}/api/annotations`)).json();
    if (checkIfEventExists.find(e => e.tags[0] === event.id)) {
      return;
    }
    Object.keys(languagesPromShort).forEach(async language => {
      const cleanEvent = {
        time: event.begin,
        timeEnd: event.end,
        text: event.titles[languagesPromShort[language]],
        tags: [event.id, language]
      };
      console.log(`Adding annotation ${event.id}: ${cleanEvent.text}`);
      const result = await fetch(`https://api_key:${config.grafanaBearerToken}@${process.env.grafana_uri}/api/annotations`, {
        method: 'post',
        body: JSON.stringify(cleanEvent),
        headers: {
          'Content-Type': 'application/json'
        }
      });
      console.log(result);
    });
  });
}

async function getLoop (offersDB, requestsDB, eventsDB) {
  while (true) {
    const averages = await queryPrometheus();
    offers = addAverages(addLanguages(await (await offersDB.find({ active: true })).toArray()), averages);
    requests = addAverages(addLanguages(await (await requestsDB.find({ active: true })).toArray()), averages);
    offerscsv = dataToCSV(offers);
    requestscsv = dataToCSV(requests);
    events = await (await eventsDB.find({})).toArray();
    await registerEvents(events);

    offers.forEach(offer => registerMetric(client, offer, 'Offers'));
    requests.forEach(request => registerMetric(client, request, 'Requests'));
    console.log(getTime(), 'Data updated');
    await sleep(600000);
  }
}
