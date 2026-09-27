package dev.ujhhgtg.wekit.features.items.system.servers

import android.content.ContentValues
import androidx.activity.ComponentActivity
import androidx.compose.material3.Text
import androidx.compose.material3.TextField
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.res.stringResource
import dev.ujhhgtg.wekit.BuildConfig
import dev.ujhhgtg.wekit.R
import dev.ujhhgtg.wekit.features.api.core.WeDatabaseListenerApi
import dev.ujhhgtg.wekit.features.core.ClickableFeature
import dev.ujhhgtg.wekit.features.core.FeatureCategoryIds
import dev.ujhhgtg.wekit.features.items.system.localizedSystemString
import dev.ujhhgtg.wekit.preferences.WePrefs.Companion.prefOption
import dev.ujhhgtg.wekit.ui.content.AlertDialogContent
import dev.ujhhgtg.wekit.ui.content.Button
import dev.ujhhgtg.wekit.ui.content.DefaultColumn
import dev.ujhhgtg.wekit.ui.content.TextButton
import dev.ujhhgtg.wekit.ui.utils.showComposeDialog
import dev.ujhhgtg.wekit.utils.WeLogger
import dev.ujhhgtg.wekit.utils.android.showToast
import dev.ujhhgtg.wekit.utils.strings.isGroupChatWxId
import dev.ujhhgtg.wekit.utils.strings.stripWxId
import org.json.JSONArray
import org.json.JSONObject
import org.json.JSONTokener
import java.io.BufferedInputStream
import java.io.InputStream
import java.io.OutputStream
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.net.Socket
import java.net.SocketException
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

/**
 * Minimal WeChat MCP server.
 *
 * Deliberately dependency-light: a plain [ServerSocket] accept loop feeding one thread per
 * connection, with hand-rolled HTTP/1.1 + MCP JSON-RPC (org.json). No Ktor, no coroutine
 * server engine — the Ktor CIO implementation this replaces was the only feature observed to
 * be capable of pinning the host process at 250%+ CPU, so this build trades breadth for a
 * transport that cannot spin.
 *
 * Exposes read/send/query tools only:
 *   send-text-message, get-chat-history, get-contacts, get-group-members, lookup-contact-id,
 *   get-contact-detail, get-self-info, get-current-talker, wait-for-new-message
 */
object ApiServer : ClickableFeature() {

    override val technicalId = "API + MCP 服务器"
    override val nameRes = R.string.feature_api_server_name
    override val categoryIds = listOf(FeatureCategoryIds.SYSTEM_PRIVACY)
    override val descriptionRes = R.string.feature_api_server_description

    private var authToken by prefOption("api_auth_token", "your_token")
    private var serverPort by prefOption("api_port", 3001)

    private const val TAG = "ApiServer"
    private const val MCP_PATH = "/mcp"
    private const val PROTOCOL_VERSION = "2025-06-18"
    private const val SERVER_NAME = "wechat-mcp-server"
    private const val MAX_HEADER_BYTES = 32 * 1024
    private const val MAX_BODY_BYTES = 8 * 1024 * 1024
    private const val SOCKET_TIMEOUT_MS = 600_000
    private const val DEFAULT_WAIT_MS = 60_000L
    private const val MAX_WAIT_MS = 600_000L

    @Volatile private var serverSocket: ServerSocket? = null
    @Volatile private var running = false
    private var acceptThread: Thread? = null
    private val workers = Executors.newCachedThreadPool { runnable ->
        Thread(runnable, "wekit-mcp").apply { isDaemon = true }
    }
    private val sessions = ConcurrentHashMap.newKeySet<String>()

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    override fun onEnable() {
        val port = serverPort
        try {
            val socket = ServerSocket()
            socket.reuseAddress = true
            socket.bind(InetSocketAddress("127.0.0.1", port), 32)
            serverSocket = socket
            running = true
        } catch (t: Throwable) {
            WeLogger.e(TAG, "bind :$port failed: ${t.message}")
            showToast(localizedSystemString(R.string.system_api_server_invalid_port))
            return
        }

        acceptThread = Thread({ acceptLoop() }, "wekit-mcp-accept").apply {
            isDaemon = true
            start()
        }
        WeLogger.i(TAG, "MCP server listening on 127.0.0.1:$port")
        showToast(localizedSystemString(R.string.system_api_server_mcp_started, port))
    }

    override fun onDisable() {
        running = false
        runCatching { serverSocket?.close() }
        serverSocket = null
        acceptThread = null
        WeLogger.i(TAG, "MCP server stopped")
        showToast(localizedSystemString(R.string.system_api_server_stopped))
    }

    override fun onClick(context: ComponentActivity) {
        showComposeDialog(context) {
            var tokenInput by remember { mutableStateOf(authToken) }
            var portInput by remember { mutableStateOf(serverPort.toString()) }

            AlertDialogContent(
                title = { Text(stringResource(R.string.feature_api_server_name)) },
                text = {
                    DefaultColumn {
                        TextField(
                            value = tokenInput,
                            onValueChange = { tokenInput = it },
                            label = { Text(stringResource(R.string.system_api_server_auth_token)) })
                        TextField(
                            value = portInput,
                            onValueChange = { portInput = it },
                            label = { Text(stringResource(R.string.system_api_server_port)) })
                    }
                },
                dismissButton = {
                    TextButton(onDismiss) { Text(stringResource(R.string.dialog_cancel)) }
                },
                confirmButton = {
                    Button(onClick = {
                        val parsed = portInput.toIntOrNull()
                        if (parsed == null || parsed !in 1024..65535) {
                            showToast(localizedSystemString(R.string.system_api_server_invalid_port))
                            return@Button
                        }
                        val restart = parsed != serverPort
                        ApiServer.serverPort = parsed
                        ApiServer.authToken = tokenInput
                        if (restart) {
                            onDisable()
                            onEnable()
                        }
                        onDismiss()
                    }) { Text(stringResource(R.string.dialog_confirm)) }
                })
        }
    }

    // -------------------------------------------------------------------------
    // Transport
    // -------------------------------------------------------------------------

    private fun acceptLoop() {
        while (running) {
            val socket = try {
                serverSocket?.accept()
            } catch (t: Throwable) {
                if (running) WeLogger.e(TAG, "accept failed: ${t.message}")
                break
            } ?: break
            try {
                workers.execute { handleConnection(socket) }
            } catch (t: Throwable) {
                runCatching { socket.close() }
            }
        }
    }

    private class Reply(val status: Int, val body: String?, val headers: Map<String, String> = emptyMap())

    private fun handleConnection(socket: Socket) {
        try {
            socket.use { s ->
                s.soTimeout = SOCKET_TIMEOUT_MS
                val input = BufferedInputStream(s.getInputStream())
                val output = s.getOutputStream()
                val head = readHead(input) ?: return
                val lines = head.split("\r\n")
                val requestLine = lines.firstOrNull().orEmpty().split(" ")
                if (requestLine.size < 2) {
                    writeReply(output, Reply(400, """{"error":"bad request line"}"""))
                    return
                }
                val method = requestLine[0].uppercase()
                val path = requestLine[1].substringBefore('?')
                val headers = HashMap<String, String>()
                for (i in 1 until lines.size) {
                    val line = lines[i]
                    val idx = line.indexOf(':')
                    if (idx > 0) headers[line.substring(0, idx).trim().lowercase()] = line.substring(idx + 1).trim()
                }

                if (path != MCP_PATH) {
                    writeReply(output, Reply(404, """{"error":"not found"}"""))
                    return
                }
                when (method) {
                    "OPTIONS" -> writeReply(output, Reply(204, null, mapOf("Allow" to "POST, GET, DELETE, OPTIONS")))
                    "GET" -> writeReply(
                        output,
                        Reply(405, """{"error":"SSE stream not supported; use POST"}""", mapOf("Allow" to "POST, DELETE")),
                    )
                    "DELETE" -> {
                        headers["mcp-session-id"]?.let { sessions.remove(it) }
                        writeReply(output, Reply(204, null))
                    }
                    "POST" -> handlePost(input, output, headers)
                    else -> writeReply(
                        output,
                        Reply(405, """{"error":"method not allowed"}""", mapOf("Allow" to "POST, DELETE")),
                    )
                }
            }
        } catch (t: SocketException) {
            // client went away — normal
        } catch (t: Throwable) {
            WeLogger.e(TAG, "connection handler failed: ${t.message}")
        }
    }

    private fun handlePost(input: InputStream, output: OutputStream, headers: Map<String, String>) {
        if (!authorized(headers)) {
            writeReply(output, Reply(401, """{"error":"unauthorized"}""", mapOf("WWW-Authenticate" to "Bearer")))
            return
        }
        val contentLength = headers["content-length"]?.toIntOrNull() ?: -1
        if (contentLength < 0 || contentLength > MAX_BODY_BYTES) {
            writeReply(output, Reply(411, """{"error":"length required"}"""))
            return
        }
        val body = readExactly(input, contentLength) ?: return
        val text = String(body, Charsets.UTF_8)

        val extra = HashMap<String, String>()
        val payload = try {
            when (val parsed = JSONTokener(text).nextValue()) {
                is JSONArray -> {
                    val out = JSONArray()
                    for (i in 0 until parsed.length()) {
                        val item = parsed.optJSONObject(i) ?: continue
                        dispatch(item, extra)?.let { out.put(it) }
                    }
                    if (out.length() == 0) null else out.toString()
                }
                is JSONObject -> dispatch(parsed, extra)
                else -> errorReply(null, -32700, "Parse error")
            }
        } catch (t: Throwable) {
            errorReply(null, -32700, t.message ?: "parse error")
        }

        if (payload == null) {
            writeReply(output, Reply(202, null, extra))
        } else {
            writeReply(output, Reply(200, payload, extra + mapOf("Content-Type" to "application/json")))
        }
    }

    private fun authorized(headers: Map<String, String>): Boolean {
        val expected = authToken
        if (expected.isEmpty()) return true
        val presented = headers["authorization"]?.removePrefix("Bearer ")?.trim().orEmpty()
        return presented == expected
    }

    // -------------------------------------------------------------------------
    // MCP / JSON-RPC
    // -------------------------------------------------------------------------

    /** Returns the JSON-RPC response body, or null when the message needs no response (notification). */
    private fun dispatch(request: JSONObject, extraHeaders: MutableMap<String, String>): String? {
        val method = request.optString("method", "")
        if (!request.has("id") || request.isNull("id")) return null // notification / response
        val id = request.get("id")
        return when (method) {
            "initialize" -> {
                val requested = request.optJSONObject("params")?.optString("protocolVersion").orEmpty()
                val sessionId = UUID.randomUUID().toString()
                sessions.add(sessionId)
                extraHeaders["Mcp-Session-Id"] = sessionId
                val result = JSONObject()
                    .put("protocolVersion", requested.ifEmpty { PROTOCOL_VERSION })
                    .put("capabilities", JSONObject().put("tools", JSONObject().put("listChanged", false)))
                    .put(
                        "serverInfo",
                        JSONObject()
                            .put("name", SERVER_NAME)
                            .put("version", BuildConfig.VERSION_NAME)
                            .put("title", "WeChat MCP Server"),
                    )
                jsonRpcResult(id, result)
            }
            "ping" -> jsonRpcResult(id, JSONObject())
            "tools/list" -> jsonRpcResult(id, JSONObject().put("tools", toolsJson()))
            "tools/call" -> {
                val params = request.optJSONObject("params")
                val name = params?.optString("name").orEmpty()
                val args = params?.optJSONObject("arguments") ?: JSONObject()
                if (name.isEmpty()) {
                    errorReply(id, -32602, "Invalid params: missing tool name")
                } else {
                    try {
                        jsonRpcResult(id, toolResult(invoke(name, args), false))
                    } catch (t: Throwable) {
                        jsonRpcResult(id, toolResult(t.message ?: t.toString(), true))
                    }
                }
            }
            else -> errorReply(id, -32601, "Method not found: $method")
        }
    }

    private fun jsonRpcResult(id: Any, result: JSONObject): String =
        JSONObject().put("jsonrpc", "2.0").put("id", id).put("result", result).toString()

    private fun errorReply(id: Any?, code: Int, message: String): String =
        JSONObject().put("jsonrpc", "2.0").put("id", id ?: JSONObject.NULL)
            .put("error", JSONObject().put("code", code).put("message", message)).toString()

    private fun toolResult(text: String, isError: Boolean): JSONObject =
        JSONObject()
            .put("content", JSONArray().put(JSONObject().put("type", "text").put("text", text)))
            .put("isError", isError)

    private fun toolsJson(): JSONArray {
        val array = JSONArray()
        for (spec in TOOLS) {
            val properties = JSONObject()
            for ((propName, propType, propDescription) in spec.props) {
                properties.put(propName, JSONObject().put("type", propType).put("description", propDescription))
            }
            val schema = JSONObject().put("type", "object").put("properties", properties)
            schema.put("required", JSONArray(spec.required))
            array.put(
                JSONObject()
                    .put("name", spec.name)
                    .put("description", spec.description)
                    .put("inputSchema", schema),
            )
        }
        return array
    }

    private fun invoke(name: String, args: JSONObject): String {
        val spec = TOOLS.firstOrNull { it.name == name } ?: throw IllegalArgumentException("Unknown tool: $name")
        return spec.handler(args)
    }

    // -------------------------------------------------------------------------
    // Tools
    // -------------------------------------------------------------------------

    private class ToolSpec(
        val name: String,
        val description: String,
        val props: List<Triple<String, String, String>>,
        val required: List<String>,
        val handler: (JSONObject) -> String,
    )

    private inline fun <T> WeChatService.Result<T>.unwrap(): T = when (this) {
        is WeChatService.Result.Success -> data
        is WeChatService.Result.Error -> throw IllegalArgumentException(message)
    }

    private fun JSONObject.requiredString(key: String): String =
        optString(key).takeIf { it.isNotEmpty() } ?: throw IllegalArgumentException("Invalid $key")

    private val TOOLS: List<ToolSpec> by lazy {
        listOf(
            ToolSpec(
                name = "send-text-message",
                description = "Send a text message to a specific conversation",
                props = listOf(
                    Triple("conv-id", "string", "Conversation ID of target"),
                    Triple("content", "string", "Content of message"),
                ),
                required = listOf("conv-id", "content"),
                handler = { args ->
                    val convId = args.requiredString("conv-id")
                    val content = args.requiredString("content")
                    WeChatService.sendMessage("text", convId, content).unwrap()
                    "Sent successfully"
                },
            ),
            ToolSpec(
                name = "get-chat-history",
                description = "List paged messages of specific conversation; latest messages first",
                props = listOf(
                    Triple("conv-id", "string", "Conversation ID of target"),
                    Triple("page-index", "integer", "Page index; defaults to 1, starts from 1"),
                    Triple("page-size", "integer", "Page size; defaults to 20"),
                ),
                required = listOf("conv-id"),
                handler = { args ->
                    val convId = args.requiredString("conv-id")
                    val pageIndex = args.optInt("page-index", 1)
                    val pageSize = args.optInt("page-size", 20)
                    WeChatService.listMessages(convId, pageIndex, pageSize).unwrap()
                        .joinToString("\n") { "${it.sender}: '${it.content}'" }
                },
            ),
            ToolSpec(
                name = "get-contacts",
                description = "List all contacts by type",
                props = listOf(
                    Triple("type", "string", "Type of contacts to list; can be 'all', 'friends', 'groups', 'official_accounts'"),
                ),
                required = listOf("type"),
                handler = { args ->
                    val type = args.requiredString("type")
                    WeChatService.listContacts(type).unwrap().joinToString("\n") { c ->
                        if (type == "friends") {
                            "WxId='${c.wxId}',Nickname='${c.nickname}',CustomWxId='${c.customWxId}',RemarkName='${c.remarkName}'"
                        } else {
                            "WxId='${c.wxId}',Nickname='${c.nickname}'"
                        }
                    }
                },
            ),
            ToolSpec(
                name = "get-group-members",
                description = "List all members of specific group",
                props = listOf(
                    Triple("group-id", "string", "Group ID; must end with '@chatroom'"),
                ),
                required = listOf("group-id"),
                handler = { args ->
                    val groupId = args.requiredString("group-id")
                    WeChatService.listGroupMembers(groupId).unwrap().joinToString("\n") {
                        "WxId='${it.wxId}',Nickname='${it.nickname}',CustomWxId='${it.customWxId}',RemarkName='${it.remarkName}'"
                    }
                },
            ),
            ToolSpec(
                name = "lookup-contact-id",
                description = "Get conversation (friend or group) ID (also known as wxid) by its nickname or remark name; " +
                    "friend ID starts with 'wxid_', group ID ends with '@chatroom'; " +
                    "matches by its group-specific nickname if group-id is provided",
                props = listOf(
                    Triple("display-name", "string", "Display name of target"),
                    Triple("group-id", "string", "Group ID; optional; must end with '@chatroom'"),
                ),
                required = listOf("display-name"),
                handler = { args ->
                    val displayName = args.requiredString("display-name")
                    val groupId = args.optString("group-id").takeIf { it.isNotEmpty() }
                    "WxId=${WeChatService.getConvIdByDisplayName(displayName, groupId).unwrap()}"
                },
            ),
            ToolSpec(
                name = "get-contact-detail",
                description = "Get detailed information of a contact (friend or group) by conversation ID",
                props = listOf(
                    Triple("conv-id", "string", "Conversation ID of target"),
                ),
                required = listOf("conv-id"),
                handler = { args ->
                    val convId = args.requiredString("conv-id")
                    val c = WeChatService.getContactDetail(convId).unwrap()
                    "WxId='${c.wxId}',Nickname='${c.nickname}',CustomWxId='${c.customWxId}',RemarkName='${c.remarkName}'," +
                        "DisplayName='${c.displayName}',AvatarUrl='${c.avatarUrl}',Type=${c.type},IsGroup=${c.isGroup}"
                },
            ),
            ToolSpec(
                name = "get-self-info",
                description = "Get details of the currently logged-in user profile",
                props = emptyList(),
                required = emptyList(),
                handler = {
                    val self = WeChatService.getSelfInfo().unwrap()
                    "WxId='${self.wxId}',CustomWxId='${self.customWxId}'"
                },
            ),
            ToolSpec(
                name = "get-current-talker",
                description = "Get current conversation ID of the active chat window",
                props = emptyList(),
                required = emptyList(),
                handler = { WeChatService.getCurrentTalker().unwrap() },
            ),
            ToolSpec(
                name = "wait-for-new-message",
                description = "Block until a new incoming message arrives, then return it. " +
                    "If conv-id is given, only messages from that conversation are awaited; " +
                    "otherwise the first incoming message from any conversation is returned. " +
                    "Self-sent messages are ignored. Returns nothing if no message arrives before the timeout.",
                props = listOf(
                    Triple("conv-id", "string", "Optional conversation ID to wait on; waits on any conversation if omitted"),
                    Triple("timeout-ms", "integer", "Optional max time to wait in milliseconds; defaults to 60000"),
                ),
                required = emptyList(),
                handler = { args ->
                    val convId = args.optString("conv-id").takeIf { it.isNotEmpty() }
                    val timeoutMs = args.optLong("timeout-ms", DEFAULT_WAIT_MS).coerceIn(1_000L, MAX_WAIT_MS)
                    awaitNewMessage(convId, timeoutMs)
                },
            ),
        )
    }

    /** Blocks the calling connection thread until a matching incoming message row is inserted. */
    private fun awaitNewMessage(convId: String?, timeoutMs: Long): String {
        val latch = CountDownLatch(1)
        val holder = AtomicReference<ContentValues?>(null)
        val listener = WeDatabaseListenerApi.IInsertListener { table, values ->
            if (latch.count == 0L) return@IInsertListener
            if (table != "message") return@IInsertListener
            if ((values.getAsInteger("isSend") ?: 0) == 1) return@IInsertListener
            val talker = values.getAsString("talker") ?: return@IInsertListener
            if (convId != null && talker != convId) return@IInsertListener
            if (holder.compareAndSet(null, values)) latch.countDown()
        }

        WeDatabaseListenerApi.addListener(listener)
        val arrived = try {
            latch.await(timeoutMs, TimeUnit.MILLISECONDS)
        } finally {
            WeDatabaseListenerApi.removeListener(listener)
        }
        val values = holder.get()
        if (!arrived || values == null) return "No new message arrived within ${timeoutMs}ms"

        val talker = values.getAsString("talker").orEmpty()
        val type = values.getAsInteger("type") ?: 0
        val rawContent = values.getAsString("content").orEmpty()
        val sender: String
        val content: String
        if (talker.isGroupChatWxId) {
            sender = rawContent.substringBefore(':', "").ifEmpty { talker }
            content = rawContent.stripWxId()
        } else {
            sender = talker
            content = rawContent
        }
        return "ConvId='$talker',Sender='$sender',Type=$type,Content='$content'"
    }

    // -------------------------------------------------------------------------
    // HTTP helpers
    // -------------------------------------------------------------------------

    private fun readHead(input: InputStream): String? {
        val buffer = StringBuilder()
        var state = 0
        while (buffer.length < MAX_HEADER_BYTES) {
            val b = input.read()
            if (b < 0) return if (state == 0) null else buffer.toString()
            buffer.append(b.toChar())
            state = when {
                b == '\r'.code && (state == 0 || state == 2) -> state + 1
                b == '\n'.code && state == 1 -> 2
                b == '\n'.code && state == 3 -> 4
                else -> 0
            }
            if (state == 4) return buffer.toString()
        }
        return null
    }

    private fun readExactly(input: InputStream, length: Int): ByteArray? {
        val out = ByteArray(length)
        var offset = 0
        while (offset < length) {
            val read = input.read(out, offset, length - offset)
            if (read < 0) return if (offset == 0) null else out.copyOf(offset)
            offset += read
        }
        return out
    }

    private fun writeReply(output: OutputStream, reply: Reply) {
        val body = reply.body?.toByteArray(Charsets.UTF_8)
        val head = StringBuilder("HTTP/1.1 ${reply.status} ${statusText(reply.status)}\r\n")
        head.append("Content-Length: ").append(body?.size ?: 0).append("\r\n")
        head.append("Connection: close\r\n")
        head.append("Access-Control-Allow-Origin: *\r\n")
        head.append("Access-Control-Allow-Headers: Content-Type, Authorization, Mcp-Session-Id, Accept\r\n")
        head.append("Access-Control-Allow-Methods: POST, GET, DELETE, OPTIONS\r\n")
        for ((name, value) in reply.headers) head.append(name).append(": ").append(value).append("\r\n")
        head.append("\r\n")
        output.write(head.toString().toByteArray(Charsets.ISO_8859_1))
        if (body != null) output.write(body)
        output.flush()
    }

    private fun statusText(status: Int): String = when (status) {
        200 -> "OK"
        202 -> "Accepted"
        204 -> "No Content"
        400 -> "Bad Request"
        401 -> "Unauthorized"
        404 -> "Not Found"
        405 -> "Method Not Allowed"
        411 -> "Length Required"
        else -> "Error"
    }
}
