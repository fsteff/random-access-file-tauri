const RandomAccessStorage = require('random-access-storage')
const fs = require('@tauri-apps/api/fs.cjs')

function RandomAccessTauriFile (filename, basePath = fs.BaseDirectory.AppData) {
  const baseDirOpts = {dir: basePath}
  const pageSize = 4000
  let stats = {size: 0}
  
  function pageName (num) {
    return 'data/' + num + '_' + filename
  }

  function intoPages(buf, offset) {
    let pageNr = Math.floor(offset / pageSize)
    const pageOffset = offset % pageSize
    const pages = [{filename: pageName(pageNr), offset: pageOffset, data: buf.slice(pageOffset, pageSize - pageOffset)}]
    buf = buf.slice(pageSize - pageOffset)
    while (buf.length > 0) {
      pages.push({
        filename: pageName(++pageNr),
        offset: 0,
        data: buf.slice(0, pageSize)
      })
      buf = buf.slice(pageSize)
    }
    return pages
  }

  function fromPages(length, offset) {
    let pageNr = Math.floor(offset / pageSize)
    const pageOffset = offset % pageSize
    const pages = [{filename: pageName(pageNr), offset: pageOffset, length: pageSize - pageOffset}]
    length -= (pageSize - pageOffset)
    while (length > 0) {
      pages.push({
        filename: pageName(++pageNr),
        offset: 0,
        length: Math.min(length, pageSize)
      })
      length -= pageSize
    }
    return pages
  }

  function toLenBuf(buf) {
    const lenBuf = Buffer.alloc(4)
    lenBuf.writeUInt32BE(buf.length)
    return Buffer.concat([lenBuf, buf])
  }

  /** @param {Buffer} buf */
  function fromLenBuf(buf) {
    return {
      length: buf.readUInt32BE(0),
      data: buf.slice(4)
    }
  }

  async function updateStats(size) {
    const statFileName = filename + '.stats.json'
    const newStats = {
      size: size
    }
    await fs.writeTextFile(statFileName, JSON.stringify(newStats), baseDirOpts)
    return newStats
  }

  async function readStats() {
    const statFileName = filename + '.stats.json'
    try{
      return JSON.parse(await fs.readTextFile(statFileName, baseDirOpts))
    } catch (err) {
      console.warn(err)
      return {
        size: 0
      }
    }
  }

  return new RandomAccessStorage({

    open: async function (req) {
      stats = await readStats()
      if(! await fs.exists('data', {dir: basePath})) {
        try { 
            await fs.createDir('data', {dir: basePath, recursive: true})
            req.callback(null)
        } catch (err) {
            return req.callback(err)
        }
      } else {
        req.callback(null)
      }
      
    },
    read: async function (req) {
      const pages = fromPages(req.size, req.offset)
      let buf = Buffer.alloc(0)
      for(const page of pages) {
        try {
          const bytes = fromLenBuf(Buffer.from(await fs.readBinaryFile(page.filename, baseDirOpts)))
          const slice = Buffer.from(bytes.data.slice(page.offset, page.offset + bytes.length))
          buf = Buffer.concat([buf, slice])
        } catch (err) {
          console.error('failed to read file ' + page.filename + ': ' + err.message)
          return req.callback(null, buf)
        }
      }
      req.callback(null, buf)
    },
    write: async function(req) {
        const pages = intoPages(req.data, req.offset)
        for(const page of pages) {
          let buf
          try {
            buf = Buffer.from(await fs.readBinaryFile(page.filename, baseDirOpts))
          } catch (err) {
            buf = Buffer.alloc(pageSize)
            console.log('file ' + page.filename + ' not readable')
          }
          page.data.copy(buf, page.offset)
          try {
            await fs.writeBinaryFile(page.filename, toLenBuf(buf), baseDirOpts)
            console.log('successfully wrote to file ' + page.filename)
          } catch (err){
            console.error(err)
          }
        }
        stats = await updateStats(Math.max(stats.size, (req.offset + req.data.length)))
        req.callback(null, req.data.length)
    },
    stat: async function(req) {
        req.callback(null, stats)
    }, 
    close: function (req) {
      req.callback(null)
    },
    del: async function(req) {
      try {
        await fs.removeFile(filename, baseDirOpts)
        req.callback(null)
      } catch(err) {
        req.callback(err)
      }
    }
  })
}

module.exports = RandomAccessTauriFile