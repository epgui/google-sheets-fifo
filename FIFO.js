const COLUMNS = {
  BROKER: 0,
  ACCOUNT: 1,
  DATE: 2,
  TICKER: 3,
  ACTION_TYPE: 4,
  QUANTITY: 5,
  UNIT_PRICE: 6,
  BOOK_COST: 7,
  CURRENCY: 8,
  ASSET_CLASS: 9,
};

const parseRow_ = row => Object.keys(COLUMNS).reduce(
  (acc, column) => ({ ...acc, [column]: row[COLUMNS[column]] }),
  {}
);

const createIdentifier_ = ({ broker, account, ticker, currency, assetClass }) =>
  `${broker}::${account}::${ticker}::${currency}::${assetClass}`;

const parseIdentifier_ = identifier => {
  const [
    broker,
    account,
    ticker,
    currency,
    assetClass,
  ] = identifier.split("::");

  return { broker, account, ticker, currency, assetClass };
};

const createFifoQueueRecord_ = row => {
  const record = parseRow_(row);
  const actionType = record.ACTION_TYPE.toUpperCase();
  return {
    identifier: createIdentifier_({
      broker: record.BROKER.toString(),
      account: record.ACCOUNT.toString(),
      ticker: record.TICKER.toString(),
      currency: record.CURRENCY.toString(),
      assetClass: record.ASSET_CLASS.toString(),
    }),
    record: {
      actionType,
      unitPrice: Number(record.UNIT_PRICE),
      ...(actionType === "SPLIT"
        ? { splitRatio: record.QUANTITY.toString().split(":") }
        : { quantity: Number(record.QUANTITY) }
      ),
    },
  };
};

const pipe = (...fns) => x => fns.reduce((y, f) => f(y), x);

const nonEmptyRows_ = rows => rows.filter(row => !!row[COLUMNS.ACTION_TYPE]);

const removeObjectKey_ = (key, { [key]: deletedKey, ...others }) => others;

const round_ = (num, precision) => Number(num.toFixed(precision));

const handleFifoBuy_ = (queue, { splitRatio, ...trade }) =>
  queue.length === 0 ? [trade] : [...queue, trade];

const handleFifoSplit_ = (queue, { splitRatio }) => queue.map(item => ({
  ...item,
  quantity: item.quantity * (splitRatio[0] / splitRatio[1]),
  unitPrice: item.unitPrice * (splitRatio[1] / splitRatio[0]),
}));

const handleFifoSell_ = (queue, { quantity }) => {
  const sharesToSell = round_(quantity, 5);
  const itemToSellQuantity = round_(queue[0].quantity, 5);
  return itemToSellQuantity === sharesToSell
    ? queue.slice(1)
    : itemToSellQuantity < sharesToSell
      ? handleFifoSell_(
        queue.slice(1),
        { quantity: sharesToSell - itemToSellQuantity },
      ) : [
        { ...queue[0], quantity: itemToSellQuantity - sharesToSell },
        ...queue.slice(1),
      ];
};

const updateQueueWithAction = {
  "DRIP": handleFifoBuy_,
  "BUY": handleFifoBuy_,
  "SPLIT": handleFifoSplit_,
  "SELL": handleFifoSell_,
};

const computeFifoQueues_ = rows => rows.reduce(
  (queues, row) => {
    const { identifier, record } = createFifoQueueRecord_(row);
    return Object.keys(updateQueueWithAction).includes(record.actionType)
      ? {
          ...queues,
          [identifier]: updateQueueWithAction[record.actionType](
            queues[identifier] ?? [],
            record,
          ),
        }
      : queues;
  },
  {},
);

const deleteEmptyQueues_ = fifoQueues => Object.keys(fifoQueues).reduce(
  (queues, identifier) => fifoQueues[identifier]?.length === 0
    ? removeObjectKey_(identifier, queues)
    : queues,
  fifoQueues,
);

const reduceBatchQuantity_ = batches => batches.reduce(
  (total, { quantity }) => total + quantity,
  0,
);

const reduceBatchCost_ = batches => batches.reduce(
  (total, { quantity, unitPrice }) => total + (quantity * unitPrice),
  0,
);

const resolveFifoQueues_ = fifoQueues => Object.keys(fifoQueues).reduce(
  (acc, identifier) => {
    const {
      broker,
      account,
      ticker,
      currency,
      assetClass,
    } = parseIdentifier_(identifier);

    const totalQuantity = reduceBatchQuantity_(fifoQueues[identifier]);
    const avgPrice = reduceBatchCost_(fifoQueues[identifier]) / totalQuantity

    return [
      ...acc,
      [
        broker,
        account,
        ticker,
        totalQuantity,
        avgPrice,
        currency,
        assetClass,
      ],
    ];
  },
  [],
);

const myPositions = rows => pipe(
  nonEmptyRows_,
  computeFifoQueues_,
  deleteEmptyQueues_,
  resolveFifoQueues_,
)(rows);
