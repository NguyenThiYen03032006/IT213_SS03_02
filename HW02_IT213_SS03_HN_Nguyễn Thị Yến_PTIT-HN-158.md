# ĐỌC HIỂU & DÒ LỖI — ENDPOINT STREAM “GIẢ SSE”

## 1. Phân tích nguyên nhân lỗi

### 1.1. Đoạn mã nguồn ban đầu

```java
@GetMapping("/api/v1/ai/stream")
public Flux<String> getStreamResponse(@RequestParam String message) {
    return chatModel.stream(new Prompt(message))
            .map(response -> response.getResult().getOutput().getText());
}
```

Về mặt ý tưởng, đoạn code trên **đã sử dụng `Flux` và phương thức `chatModel.stream()`**, do đó có khả năng phát dữ liệu theo từng phần.

Tuy nhiên, để HTTP endpoint thực sự hoạt động như **Server-Sent Events (SSE)**, cần khai báo rõ kiểu nội dung trả về là:

```text
text/event-stream
```

Trong Spring WebFlux, cách phổ biến là khai báo:

```java
produces = MediaType.TEXT_EVENT_STREAM_VALUE
```

Nếu không khai báo, Spring có thể lựa chọn cơ chế serialization thông thường cho `Flux<String>` thay vì xử lý response như một SSE stream.

### 1.2. Lỗi cốt lõi

Lỗi chính trong Controller là:

```java
@GetMapping("/api/v1/ai/stream")
```

chưa chỉ rõ:

```java
produces = MediaType.TEXT_EVENT_STREAM_VALUE
```

Nên sửa thành:

```java
@GetMapping(
    value = "/api/v1/ai/stream",
    produces = MediaType.TEXT_EVENT_STREAM_VALUE
)
```

Tuy nhiên, **chỉ thêm `produces` chưa chắc giải quyết toàn bộ vấn đề**.

Cần kiểm tra thêm các yếu tố sau:

1. `chatModel.stream(...)` có thực sự trả về dữ liệu theo từng chunk hay không.
2. Client có sử dụng cơ chế đọc SSE hay không.
3. Không được gọi các API blocking trong pipeline WebFlux.
4. Không được gom toàn bộ `Flux` thành một `String` trước khi trả response.
5. Mỗi chunk cần được chuyển thành dữ liệu hợp lệ và không bị `null`.
6. HTTP response phải được giữ mở để server tiếp tục gửi các event.
7. Web server/proxy không được buffer toàn bộ response trước khi gửi cho client.

---

# 2. Cơ chế hoạt động của SSE trong Spring WebFlux

## 2.1. SSE là gì?

Server-Sent Events (SSE) là cơ chế cho phép:

> Server mở một HTTP connection và chủ động gửi nhiều event về client theo thời gian.

Khác với REST API thông thường:

```text
Client
   |
   | HTTP Request
   v
Server
   |
   | xử lý 20 giây
   |
   | trả toàn bộ response
   v
Client
```

SSE hoạt động theo mô hình:

```text
Client
   |
   | HTTP Request
   v
Server
   |
   |---- event 1 ----> Client
   |
   |---- event 2 ----> Client
   |
   |---- event 3 ----> Client
   |
   |---- event 4 ----> Client
   |
   |---- event 5 ----> Client
   |
   |---- complete ---> Client
```

Ví dụ server có thể gửi:

```text
data: Xin

data: chào

data: bạn

data: !
```

Client có thể hiển thị ngay:

```text
Xin
Xin chào
Xin chào bạn
Xin chào bạn!
```

thay vì phải chờ toàn bộ câu trả lời được tạo xong.

---

# 3. `Flux` đóng vai trò gì?

Trong Spring WebFlux, `Flux<T>` biểu diễn một chuỗi gồm **0 hoặc nhiều phần tử được phát ra theo thời gian**.

Ví dụ:

```java
Flux<String> stream = Flux.just(
    "Hello",
    " ",
    "World"
);
```

Có thể hình dung:

```text
Flux
 |
 +---- "Hello"
 |
 +---- " "
 |
 +---- "World"
 |
 +---- complete
```

Đây là điểm rất quan trọng đối với chatbot.

Nếu LLM trả về:

```text
"Xin"
" chào"
" bạn"
"!"
```

thì WebFlux có thể truyền từng phần tử về client thay vì chờ:

```text
"Xin chào bạn!"
```

---

# 4. `chatModel.stream()` hoạt động như thế nào?

Đoạn code:

```java
chatModel.stream(new Prompt(message))
```

về mặt ý tưởng sẽ tạo ra một:

```text
Flux<ChatResponse>
```

Ví dụ:

```text
ChatResponse #1
    ↓
"Xin"

ChatResponse #2
    ↓
" chào"

ChatResponse #3
    ↓
" bạn"

ChatResponse #4
    ↓
"!"
```

Sau đó:

```java
.map(response ->
    response.getResult()
            .getOutput()
            .getText()
)
```

chuyển:

```text
Flux<ChatResponse>
```

thành:

```text
Flux<String>
```

Luồng dữ liệu sẽ là:

```text
chatModel.stream()
       |
       v
Flux<ChatResponse>
       |
       | map()
       v
Flux<String>
       |
       v
HTTP SSE Response
       |
       v
Client
```

Nếu `chatModel.stream()` thực sự phát từng chunk, WebFlux có thể gửi từng chunk đó tới client.

---

# 5. Vì sao client lại phải chờ 20 giây?

Có một số nguyên nhân có thể dẫn đến hiện tượng này.

## 5.1. Không khai báo `text/event-stream`

Đây là lỗi cấu hình dễ thấy nhất.

Code ban đầu:

```java
@GetMapping("/api/v1/ai/stream")
```

Nên khai báo:

```java
@GetMapping(
    value = "/api/v1/ai/stream",
    produces = MediaType.TEXT_EVENT_STREAM_VALUE
)
```

`MediaType.TEXT_EVENT_STREAM_VALUE` tương ứng với:

```text
text/event-stream
```

Header HTTP sẽ có dạng:

```http
Content-Type: text/event-stream
```

Client lúc này biết rằng response là một SSE stream và không nên chờ toàn bộ response mới xử lý.

---

# 6. Không được biến `Flux` thành dữ liệu hoàn chỉnh trước khi trả về

Một lỗi phổ biến là làm như sau:

```java
String result = chatModel.stream(new Prompt(message))
        .map(...)
        .collectList()
        .map(list -> String.join("", list))
        .block();
```

Hoặc:

```java
String result = chatModel.stream(...)
        .reduce("", (a, b) -> a + b)
        .block();
```

Cách này phá vỡ hoàn toàn ý nghĩa của streaming.

Luồng xử lý trở thành:

```text
LLM
 |
 | chunk 1
 | chunk 2
 | chunk 3
 | ...
 v
gom toàn bộ dữ liệu
 |
 v
block()
 |
 v
HTTP Response
```

Client sẽ phải chờ toàn bộ câu trả lời.

Với WebFlux, Controller nên trả trực tiếp:

```java
Flux<String>
```

thay vì:

```java
String
```

---

# 7. Không được sử dụng `block()` trong Controller WebFlux

Đặc biệt không nên viết:

```java
.block()
```

trong Controller.

Ví dụ sai:

```java
@GetMapping(...)
public String stream(...) {
    return chatModel.stream(...)
            .map(...)
            .block();
}
```

`block()` sẽ biến xử lý reactive thành blocking.

Điều này đi ngược lại mô hình non-blocking của WebFlux.

Cách đúng:

```java
@GetMapping(...)
public Flux<String> stream(...) {
    return chatModel.stream(...)
            .map(...);
}
```

---

# 8. Vấn đề `null` hoặc chunk rỗng

Đoạn:

```java
.map(response -> response.getResult().getOutput().getText())
```

cũng cần cẩn thận.

Không phải mọi `ChatResponse` đều chắc chắn có text.

Có thể xảy ra:

```text
response
   |
   +-- result = null
   |
   +-- output = null
   |
   +-- text = null
```

Nếu không kiểm tra, ứng dụng có thể gặp:

```text
NullPointerException
```

Ngoài ra, một số chunk có thể không chứa nội dung text.

Do đó nên lọc:

```java
.filter(Objects::nonNull)
.filter(text -> !text.isBlank())
```

hoặc kiểm tra cấu trúc response trước khi lấy text.

---

# 9. SSE thực chất truyền dữ liệu như thế nào?

SSE sử dụng HTTP connection lâu dài.

Header phản hồi thường có:

```http
HTTP/1.1 200 OK
Content-Type: text/event-stream
Cache-Control: no-cache
Connection: keep-alive
```

Server gửi các event theo format:

```text
data: Xin chào

data: Tôi có thể hỗ trợ bạn

data: tra cứu quy trình thông quan.

```

Mỗi event SSE được kết thúc bằng một dòng trống.

Có thể hiểu:

```text
data: chunk 1
\n
data: chunk 2
\n
data: chunk 3
\n
```

WebFlux sẽ chịu trách nhiệm serialize các phần tử của `Flux` thành response stream phù hợp.

---

# 10. Controller hoàn chỉnh sau khi sửa

Một phiên bản đơn giản, phù hợp với Spring WebFlux và Spring AI có thể viết như sau:

```java
package com.rlogistics.chatbot.controller;

import org.springframework.ai.chat.model.ChatResponse;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.chat.prompt.Prompt;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

@RestController
public class AiStreamController {

    private final ChatModel chatModel;

    public AiStreamController(ChatModel chatModel) {
        this.chatModel = chatModel;
    }

    @GetMapping(
        value = "/api/v1/ai/stream",
        produces = MediaType.TEXT_EVENT_STREAM_VALUE
    )
    public Flux<String> getStreamResponse(
            @RequestParam String message
    ) {

        return chatModel.stream(new Prompt(message))
                .map(ChatResponse::getResult)
                .filter(result -> result != null)
                .map(result -> result.getOutput())
                .filter(output -> output != null)
                .map(output -> output.getText())
                .filter(text -> text != null && !text.isBlank());
    }
}
```

Luồng hoạt động:

```text
GET /api/v1/ai/stream?message=...

             |
             v

      ChatModel.stream()
             |
             v

       Flux<ChatResponse>
             |
             v
          map()
             |
             v
         Flux<String>
             |
             v
     text/event-stream
             |
             v
           Client
```

---

# 11. Phiên bản tối ưu hơn

Có thể bổ sung xử lý lỗi và log:

```java
package com.rlogistics.chatbot.controller;

import org.springframework.ai.chat.model.ChatResponse;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.chat.prompt.Prompt;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

@RestController
public class AiStreamController {

    private final ChatModel chatModel;

    public AiStreamController(ChatModel chatModel) {
        this.chatModel = chatModel;
    }

    @GetMapping(
        value = "/api/v1/ai/stream",
        produces = MediaType.TEXT_EVENT_STREAM_VALUE
    )
    public Flux<String> getStreamResponse(
            @RequestParam String message
    ) {

        if (message == null || message.isBlank()) {
            return Flux.error(
                new IllegalArgumentException("Message không được để trống")
            );
        }

        return chatModel.stream(new Prompt(message))
                .map(ChatResponse::getResult)
                .filter(result -> result != null)
                .map(result -> result.getOutput())
                .filter(output -> output != null)
                .map(output -> output.getText())
                .filter(text -> text != null && !text.isBlank())
                .doOnNext(text ->
                    System.out.println("Sending chunk: " + text)
                )
                .doOnComplete(() ->
                    System.out.println("AI stream completed")
                )
                .doOnError(error ->
                    System.err.println(
                        "AI stream error: " + error.getMessage()
                    )
                );
    }
}
```

Điểm quan trọng là **không sử dụng**:

```java
.block()
```

và cũng không sử dụng:

```java
.collectList()
```

để gom toàn bộ dữ liệu trước khi trả về.

---

# 12. Có thể sử dụng `ServerSentEvent<String>` để rõ ràng hơn

Nếu muốn biểu diễn SSE một cách tường minh, Controller có thể trả về:

```java
Flux<ServerSentEvent<String>>
```

Code:

```java
package com.rlogistics.chatbot.controller;

import org.springframework.ai.chat.model.ChatResponse;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.chat.prompt.Prompt;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

@RestController
public class AiStreamController {

    private final ChatModel chatModel;

    public AiStreamController(ChatModel chatModel) {
        this.chatModel = chatModel;
    }

    @GetMapping(
        value = "/api/v1/ai/stream",
        produces = MediaType.TEXT_EVENT_STREAM_VALUE
    )
    public Flux<ServerSentEvent<String>> getStreamResponse(
            @RequestParam String message
    ) {

        return chatModel.stream(new Prompt(message))
                .map(ChatResponse::getResult)
                .filter(result -> result != null)
                .map(result -> result.getOutput())
                .filter(output -> output != null)
                .map(output -> output.getText())
                .filter(text -> text != null && !text.isBlank())
                .map(text ->
                    ServerSentEvent.<String>builder()
                        .data(text)
                        .build()
                );
    }
}
```

Cách này phù hợp khi muốn kiểm soát rõ hơn cấu trúc SSE.

Có thể mở rộng event:

```java
ServerSentEvent.<String>builder()
    .event("message")
    .data(text)
    .build();
```

Khi đó client nhận được event có tên:

```text
message
```

---

# 13. Kiểm tra phía Client

Nếu sử dụng trình duyệt, có thể kiểm tra SSE bằng:

```javascript
const eventSource = new EventSource(
    "/api/v1/ai/stream?message=Xin%20chao"
);

eventSource.onmessage = (event) => {
    console.log("Chunk:", event.data);
};

eventSource.onerror = (error) => {
    console.error("SSE error:", error);
    eventSource.close();
};
```

Nếu server thực sự streaming, console sẽ nhận:

```text
Chunk: Xin
Chunk:  chào
Chunk: bạn
Chunk: !
```

thay vì:

```text
Chunk: Xin chào bạn!
```

sau 20 giây.

---

# 14. Một vấn đề quan trọng: `chatModel.stream()` phải thực sự streaming

Việc Controller trả:

```java
Flux<String>
```

không có nghĩa chắc chắn LLM đang streaming.

Ví dụ:

```text
chatModel.stream()
```

nếu bên dưới thực chất làm:

```text
Gọi LLM
   ↓
Chờ LLM tạo xong toàn bộ response
   ↓
Nhận full response
   ↓
Đóng gói thành Flux
```

thì Controller vẫn không thể tạo ra streaming thật.

Khi đó:

```text
Flux
```

chỉ là "vỏ reactive" bên ngoài một thao tác vốn đã blocking/chờ toàn bộ response.

Muốn streaming thực sự:

```text
LLM Provider
     |
     | token/chunk 1
     | token/chunk 2
     | token/chunk 3
     | ...
     v
Spring AI ChatModel
     |
     v
Flux<ChatResponse>
     |
     v
Spring WebFlux
     |
     v
SSE
     |
     v
Browser
```

---

# 15. Những nguyên nhân cần kiểm tra nếu đã thêm `produces` nhưng vẫn bị blocking

Nếu sửa:

```java
produces = MediaType.TEXT_EVENT_STREAM_VALUE
```

mà vẫn phải chờ 20 giây thì cần kiểm tra tiếp:

### Nguyên nhân 1 — LLM provider không bật streaming

Ví dụ cấu hình/model không sử dụng streaming thực sự.

Cần kiểm tra cấu hình Spring AI và API provider đang sử dụng.

### Nguyên nhân 2 — Client không đọc SSE đúng cách

Client phải sử dụng:

```javascript
new EventSource(...)
```

hoặc cơ chế đọc streaming tương ứng.

Nếu frontend sử dụng:

```javascript
const response = await fetch(url);
const data = await response.json();
```

thì frontend có thể chờ toàn bộ response rồi mới xử lý.

### Nguyên nhân 3 — Có proxy buffering

Nếu triển khai qua:

```text
Nginx
Load Balancer
API Gateway
Cloud Proxy
```

proxy có thể buffer response.

Khi đó:

```text
Spring WebFlux
     |
     | chunk 1
     | chunk 2
     | chunk 3
     v
Proxy
     |
     | buffer
     |
     | chờ đủ dữ liệu
     v
Client
```

Kết quả nhìn từ client giống như REST API blocking.

### Nguyên nhân 4 — Có code blocking trong pipeline

Ví dụ:

```java
.block()
```

hoặc:

```java
.blockFirst()
```

hoặc:

```java
.blockLast()
```

hoặc các thao tác I/O blocking khác.

Đây là điều cần tránh trong WebFlux.

### Nguyên nhân 5 — Gom toàn bộ Flux

Ví dụ:

```java
.collectList()
```

hoặc:

```java
.reduce(...)
```

rồi mới trả response.

Điều này cũng phá vỡ streaming.

---

# 16. So sánh REST thông thường và SSE

| REST thông thường            | SSE                           |
| ---------------------------- | ----------------------------- |
| Chờ xử lý xong               | Có thể nhận từng phần         |
| Response thường trả một lần  | Response được gửi nhiều lần   |
| Phù hợp dữ liệu hoàn chỉnh   | Phù hợp dữ liệu phát liên tục |
| Client nhận toàn bộ response | Client nhận từng event        |
| Có thể trả `String`, DTO     | Thường trả `Flux`             |
| `Content-Type` tùy response  | `text/event-stream`           |
| Không cần connection lâu dài | Giữ HTTP connection mở        |

---

# 17. Luồng xử lý đúng của hệ thống R-Logistics

```text
                 CLIENT
                    |
                    | GET /api/v1/ai/stream
                    | message=...
                    v
          ┌─────────────────────┐
          │ Spring WebFlux      │
          │ Controller          │
          └──────────┬──────────┘
                     |
                     | Prompt
                     v
          ┌─────────────────────┐
          │ Spring AI           │
          │ ChatModel.stream()   │
          └──────────┬──────────┘
                     |
                     | chunk 1
                     | chunk 2
                     | chunk 3
                     | ...
                     v
               Flux<ChatResponse>
                     |
                     | map()
                     v
                Flux<String>
                     |
                     | SSE
                     v
          ┌─────────────────────┐
          │ HTTP Response       │
          │ text/event-stream   │
          └──────────┬──────────┘
                     |
                     | chunk 1
                     | chunk 2
                     | chunk 3
                     v
                 CLIENT
                     |
                     v
             Hiển thị từng chữ
```

---

# 18. Kết luận

Nguyên nhân cốt lõi của đoạn Controller ban đầu là endpoint chưa khai báo rõ response là **SSE (`text/event-stream`)**, đồng thời cần đảm bảo rằng `chatModel.stream()` thực sự phát dữ liệu theo từng chunk và toàn bộ pipeline không chứa thao tác blocking hoặc gom dữ liệu.

Đoạn ban đầu:

```java
@GetMapping("/api/v1/ai/stream")
public Flux<String> getStreamResponse(@RequestParam String message) {
    return chatModel.stream(new Prompt(message))
            .map(response -> response.getResult().getOutput().getText());
}
```

nên được sửa tối thiểu thành:

```java
@GetMapping(
    value = "/api/v1/ai/stream",
    produces = MediaType.TEXT_EVENT_STREAM_VALUE
)
public Flux<String> getStreamResponse(
        @RequestParam String message
) {
    return chatModel.stream(new Prompt(message))
            .map(response -> response.getResult().getOutput().getText());
}
```

Và phiên bản an toàn hơn nên có kiểm tra `null`, lọc chunk rỗng và xử lý lỗi.

Điểm quan trọng nhất cần nhớ là:

```text
Flux
+
chatModel.stream()
+
text/event-stream
+
không block()
+
không gom toàn bộ response
+
client đọc SSE
=
SSE Streaming thực sự
```

Do đó, **chỉ khai báo `Flux<String>` chưa đủ để đảm bảo SSE streaming**. Cần có cả cơ chế phát dữ liệu theo thời gian từ LLM, HTTP response với `Content-Type: text/event-stream`, WebFlux non-blocking và client thực sự đọc dữ liệu dạng stream.
