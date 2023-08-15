import './index.d'
import Pool from 'pg-pool'
import escapeSqlModule from 'sql-escape-string'
import { createClient } from 'redis'
import { randomUUID } from 'crypto'
import { createServer, IncomingMessage } from 'http'

async function connectRedis() {
    const client = createClient({ url: 'redis://172.17.0.1:6379' })
    await client.connect()
    return client
}
const escapeSql = escapeSqlModule as any

const sql = String.raw

const pgPool = new Pool({
    user: 'root',
    password: '1234',
    database: 'rinhadb',
    host: 'db',
    port: 5432
})

const consulta_re = /^\/pessoas\/(.*)/

type QueueEvent = { id: string, payload: CriarPessoaDTO, stack: string }
const queue: QueueEvent[] = []

class Response {
    constructor(
        public body: string | null,
        public attrs: {
            status: number,
            headers?: Record<string, string>
        }
    ) {}
}

async function consultaPessoa(id: string) {
    const redisClient = await connectRedis()
    const cached = await redisClient.get(id)
    if (cached) {
        redisClient.quit()
        return new Response(cached, { status: 200 })
    }
    const pgClient = await pgPool.connect()
    const { rows } = await pgClient.query(sql`
        SELECT ID, APELIDO, NOME, NASCIMENTO, STACK
        FROM PESSOAS P
        WHERE P.ID = $1::TEXT;
    `, [id])
    pgClient.release()
    if (!rows[0]) {
        redisClient.quit()
        return new Response(null, { status: 404 })
    }
    const dto = {
        id: rows[0].ID,
        apelido: rows[0].APELIDO,
        nome: rows[0].NOME,
        nascimento: rows[0].NASCIMENTO,
        stack: rows[0].STACK && rows[0].STACK.split(' ')
    }
    const payload = JSON.stringify(dto)
    await redisClient.set(id, payload)
    redisClient.quit()
    return new Response(payload, { status: 200 })
}

async function buscaPessoas(query: string) {
    const pgClient = await pgPool.connect()
    const { rows } = await pgClient.query(sql`
        SELECT ID, APELIDO, NOME, NASCIMENTO, STACK
        FROM PESSOAS P
        WHERE TO_TSQUERY('BUSCA', $1::TEXT) @@ BUSCA
        LIMIT 50;
    `, [query && escapeSql(query)])
    pgClient.release()
    const dto = rows.map(row => {
        return {
            id: row.ID,
            apelido: row.APELIDO,
            nome: row.NOME,
            nascimento: row.NASCIMENTO,
            stack: row.STACK && row.STACK.split(' ')
        }
    })
    return new Response(JSON.stringify(dto), { status: 200 })
}

interface CriarPessoaDTO {
    apelido: string
    nascimento: string
    nome: string
    stack: string[]
}

async function criaPessoa(payload: CriarPessoaDTO) {
    if (
        !payload.nascimento.match(/^\d{4}-\d{2}-\d{2}$/) ||
        !+new Date(payload.nascimento) ||
        payload.nome.length > 100 ||
        payload.apelido.length > 32 ||
        (payload.stack && (!Array.isArray(payload.stack) || payload.stack.some(t => t.length > 32)))
    ) {
        return new Response(null, { status: 400 })
    }
    const redisClient = await connectRedis()
    const apelidoCacheKey = `a/${payload.apelido}`
    const cachedApelido = await redisClient.get(apelidoCacheKey)
    if (cachedApelido) {
        redisClient.quit()
        return new Response(null, { status: 422 })
    }
    await redisClient.set(apelidoCacheKey, "1")
    const stack = payload.stack && payload.stack.join(" ")
    const id = randomUUID()
    const dto = { id, ...payload }
    let body = JSON.stringify(dto)
    queue.push({ id, payload, stack })
    await redisClient.set(id, body)
    redisClient.quit()
    return new Response(body, { status: 201, headers: { 'Location': `/pessoas/${id}` } })
}

async function contaPessoas() {
    const pgConn = await pgPool.connect()
    const { rows } = await pgConn.query(sql`SELECT COUNT(1) AS COUNT FROM PESSOAS;`)
    pgConn.release()
    return new Response(rows[0].COUNT, { status: 200 })
}

const sleep = (duration: number) => new Promise(resolve => setTimeout(resolve, duration))

async function batchInsert() {
    await sleep(5000)
    if (queue.length === 0) { return }
    const apelidos = new Set()
    let input_sql = sql`INSERT INTO PESSOAS (ID, APELIDO, NOME, NASCIMENTO, STACK) VALUES `
    while (true) {
        const event = queue.shift()
        if (!event) { break }
        const { id, payload, stack } = event
        if (apelidos.has(payload.apelido)) { continue }
        apelidos.add(payload.apelido)
        input_sql += `(${escapeSql(id)},${escapeSql(payload.apelido)},${escapeSql(payload.nome)},${escapeSql(payload.nascimento)},${stack ? escapeSql(stack) : "NULL"})`
        if (queue.length === 0) { break }
        input_sql += ','
    }
    input_sql += ` ON CONFLICT DO NOTHING;`
    const pgConn = await pgPool.connect()
    await pgConn.query(input_sql)
    pgConn.release()
}

setInterval(batchInsert, 5000)

async function handleReq(req: IncomingMessage, body: string): Promise<Response> {
    const { method } = req
    const { pathname, searchParams } = new URL(`http://0.0.0.0:80${req.url!}`)
    const path_consulta_match = consulta_re.exec(pathname)
    const isGet = method === "GET"
    const isPost = method === "POST"
    if (isGet && path_consulta_match) {
        const [, id] = path_consulta_match
        return consultaPessoa(id)
    }
    if (pathname === "/pessoas") {
        if (isGet) {
            const t = searchParams.get("t")
            if (!t) { return new Response(null, { status: 400 }) }
            return buscaPessoas(t)
        }
        if (isPost) {
            return criaPessoa(JSON.parse(body))
        }
    }
    if (pathname === "/contagem-pessoas") {
        return contaPessoas()
    }
    return new Response(null, { status: 404 })
}

const server = createServer((req, res) => {
    let body = ""
    req.on('data', chunk => body += chunk)
    req.on('end', async () => {
        try {
            const appRes = await handleReq(req, body)
            res.statusCode = appRes.attrs.status
            const headers = appRes.attrs.headers || {}
            for (const headerName in headers) {
                res.setHeader(headerName, headers[headerName])
            }
            res.end(appRes.body)
        }
        catch (e) {
            console.error(`got error: ${e.message}`)
            res.statusCode = 500
            res.end()
        }
    })
})
server.listen(80, () => console.log('server up!'))