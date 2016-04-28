import assign from 'lodash.assign'
import filter from 'lodash.filter'
import pick from 'lodash.pick'
import {BaseError} from 'make-error'

import {
  createRawObject,
  ensureArray,
  forEach
} from './utils'

// ===================================================================

export class JobExecutorError extends BaseError {}
export class UnsupportedJobType extends JobExecutorError {
  constructor (job) {
    super('Unknown job type: ' + job.type)
  }
}
export class UnsupportedVectorType extends JobExecutorError {
  constructor (vector) {
    super('Unknown vector type: ' + vector.type)
  }
}

// ===================================================================

const getItems = ({ items, values }) => items || values

const resolveItem = (item, xo, optionsMap) => {
  const { type } = item
  const resolve = resolveMap[type]

  if (!resolve) {
    throw new UnsupportedVectorType(type)
  }

  return resolveMap[type](getItems(item), optionsMap[type], xo, optionsMap)
}

// ===================================================================

const defaultProduct = (a, b) => a * b

// items = Array
export function _computeCrossProduct (items, {
  productCb = defaultProduct,
  resolve
} = {}, xo, optionsMap) {
  items = items.slice() // Copy.

  const item = items.pop()
  const values = (resolve && resolve(item, xo, optionsMap)) || item

  if (!items.length) {
    return values
  }

  const result = []
  const subValues = _computeCrossProduct(items, { productCb, resolve }, xo, optionsMap)

  forEach(values, (itemValue) => {
    forEach(subValues, (subValue) => {
      result.push(productCb(itemValue, subValue))
    })
  })

  return result
}

// items = Array
function set (items) {
  return items.slice() // Copy.
}

// items = Object: { items, properties }
function extractProperties (items, options, xo, optionsMap) {
  const itemsToFilter = ensureArray(getItems(items))
  const result = []

  forEach(itemsToFilter, (item) => {
    forEach(ensureArray(resolveItem(item, xo, optionsMap)), (item) => {
      result.push(pick(
        item,
        items.properties
      ))
    })
  })

  return result
}

// items = Object
function fetchObjects (items, options, xo) {
  return filter(xo.getObjects(), items)
}

export const mergeObjects = (...args) => assign(createRawObject(), ...args)

const resolveMap = {
  extractProperties,
  crossProduct: _computeCrossProduct,
  fetchObjects,
  set
}

// ===================================================================

const paramsVectorOptionsMap = {
  crossProduct: {
    productCb: mergeObjects,
    resolve: resolveItem
  }
}

const computeParamsVector = (paramsVector, xo, optionsMap = paramsVectorOptionsMap) => {
  return resolveItem(paramsVector, xo, optionsMap)
}

// ===================================================================

export default class JobExecutor {
  constructor (xo) {
    this.xo = xo
    this._extractValueCb = {
      'set': items => items.values
    }

    // The logger is not available until Xo has started.
    xo.on('start', () => xo.getLogger('jobs').then(logger => {
      this._logger = logger
    }))
  }

  async exec (job) {
    const runJobId = this._logger.notice(`Starting execution of ${job.id}.`, {
      event: 'job.start',
      userId: job.userId,
      jobId: job.id,
      key: job.key
    })

    try {
      if (job.type === 'call') {
        const execStatus = await this._execCall(job, runJobId)

        this.xo.emit('job:terminated', execStatus)
      } else {
        throw new UnsupportedJobType(job)
      }

      this._logger.notice(`Execution terminated for ${job.id}.`, {
        event: 'job.end',
        runJobId
      })
    } catch (e) {
      this._logger.error(`The execution of ${job.id} has failed.`, {
        event: 'job.end',
        runJobId,
        error: e
      })
    }
  }

  async _execCall (job, runJobId) {
    const { paramsVector } = job
    const paramsFlatVector = paramsVector
      ? computeParamsVector(paramsVector, this.xo)
      : [{}] // One call with no parameters

    const connection = this.xo.createUserConnection()
    const promises = []

    connection.set('user_id', job.userId)

    const execStatus = {
      runJobId,
      start: Date.now(),
      calls: {}
    }

    forEach(paramsFlatVector, params => {
      const runCallId = this._logger.notice(`Starting ${job.method} call. (${job.id})`, {
        event: 'jobCall.start',
        runJobId,
        method: job.method,
        params
      })

      const call = execStatus.calls[runCallId] = {
        method: job.method,
        params,
        start: Date.now()
      }

      promises.push(
        this.xo.api.call(connection, job.method, assign({}, params)).then(
          value => {
            this._logger.notice(`Call ${job.method} (${runCallId}) is a success. (${job.id})`, {
              event: 'jobCall.end',
              runJobId,
              runCallId,
              returnedValue: value
            })

            call.returnedValue = value
            call.end = Date.now()
          },
          reason => {
            this._logger.notice(`Call ${job.method} (${runCallId}) has failed. (${job.id})`, {
              event: 'jobCall.end',
              runJobId,
              runCallId,
              error: reason
            })

            call.error = reason
            call.end = Date.now()
          }
        )
      )
    })

    connection.close()
    await Promise.all(promises)
    execStatus.end = Date.now()

    return execStatus
  }
}
