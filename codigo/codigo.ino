/*
 * SMART-VENDING ESP32 — Versión Corregida MQTT KeepAlive
 * CORREGIDO:
 * - Evita desconexión MQTT durante espera del teclado
 * - Añadido KeepAlive extendido
 * - Añadido mqttClient.loop() durante selección de producto
 */

#include <WiFi.h>
#include <PubSubClient.h>
#include <ArduinoJson.h>
#include <SPI.h>
#include <MFRC522.h>
#include <Wire.h>
#include <LiquidCrystal_I2C.h>
#include <Keypad.h>
#include <ESP32Servo.h> 

// ====================================================================
// SELECTOR DE ENTORNO
// ====================================================================
#define ENTORNO_REMOTO
// #define ENTORNO_LOCAL
// ====================================================================

// Datos de tu red WiFi
const char* WIFI_SSID    = "Redmi Note 13";
const char* WIFI_PASS    = "JAcoBo2005";

#ifdef ENTORNO_REMOTO
    const char* MQTT_SERVER  = "broker.hivemq.com"; 
    const int   MQTT_PORT    = 1883;
    const char* MQTT_USER    = "";               
    const char* MQTT_PASS    = "";               

    const char* T_EVENTOS      = "vending/eventos";
    const char* T_REQ_USUARIO  = "vending_pro/request/usuario";
    const char* T_RESP_USUARIO = "vending_pro/response/usuario";
    const char* T_REQ_PROD     = "vending_pro/request/productos";
    const char* T_RESP_PROD    = "vending_pro/response/productos";
#endif

#ifdef ENTORNO_LOCAL
    const char* MQTT_SERVER  = "192.168.200.33";  
    const int   MQTT_PORT    = 1883;             
    const char* MQTT_USER    = "admin";          
    const char* MQTT_PASS    = "AdminPass123!";  

    const char* T_EVENTOS      = "vending/eventos";
    const char* T_REQ_USUARIO  = "vending/request/usuario";
    const char* T_RESP_USUARIO = "vending/response/usuario";
    const char* T_REQ_PROD     = "vending/request/productos";
    const char* T_RESP_PROD    = "vending/response/productos";
#endif

// Pines Hardware
#define RST_PIN 4
#define SS_PIN  5

const int PINES_SERVOS[] = {13, 2, 14, 27}; 

byte rowPins[4] = {32, 33, 25, 26};
byte colPins[4] = {15, 12, 16, 17};

Servo misServos[4]; 

// Constantes
#define MAX_PRODUCTOS       4
#define LCD_COLS            16
#define BULK_EVENTOS_MAX    5
#define BULK_FLUSH_MS       60000
#define TIMEOUT_RESP_MS     6000   
#define TIMEOUT_TECLA_MS    15000   
#define ROTATE_PRODUCTO_MS  2000   
#define SERVO_DELAY_MS      800    

// Estructuras
struct UserData {
    int  id;
    long creditos;
    bool es_nuevo;
    bool valido;      
};

struct ProductData {
    int  id;
    char nombre[20];
    long precio;
    int  stock;
};

struct VendingEvent {
    char rfid_uid[10];
    int  usuario_id;
    int  producto_id;
    long creditos_usados;
    int  stock_restante;
    long saldo_nuevo;
};

// Hardware
MFRC522 rfid(SS_PIN, RST_PIN);

LiquidCrystal_I2C lcd(0x27, 16, 2);

char keys[4][4] = {
    {'1','2','3','A'},
    {'4','5','6','B'},
    {'7','8','9','C'},
    {'*','0','#','D'}
};

Keypad teclado = Keypad(makeKeymap(keys), rowPins, colPins, 4, 4);

WiFiClient espClient;
PubSubClient mqttClient(espClient);

// FreeRTOS
QueueHandle_t logQueue;

SemaphoreHandle_t semUserRecibido;
SemaphoreHandle_t semProdRecibidos;

SemaphoreHandle_t mutexMQTT;
SemaphoreHandle_t mutexDatos;

// Variables globales
UserData datoUsuario;

ProductData productos[MAX_PRODUCTOS];
int numProductos = 0;

bool mostrarMensajeListo = false;

byte ultimoUID[4] = {0,0,0,0};
uint32_t tsUltimaLectura = 0;

#define DEBOUNCE_RFID_MS 2500

void reconnectMQTT();
void lcdPrintLine(int row, const char* text);

// ==========================================================
// CALLBACK MQTT
// ==========================================================
void mqttCallback(char* topic, byte* payload, unsigned int length) {

    StaticJsonDocument<1024> doc;

    DeserializationError err = deserializeJson(doc, payload, length);

    if (err) return;

    if (strcmp(topic, T_RESP_USUARIO) == 0) {

        if (xSemaphoreTake(mutexDatos, pdMS_TO_TICKS(100)) == pdTRUE) {

            datoUsuario.id        = doc["usuario_id"] | 0;
            datoUsuario.creditos  = doc["creditos"]   | 0L;
            datoUsuario.es_nuevo  = doc["es_nuevo"]   | false;
            datoUsuario.valido    = doc["valido"]     | false;

            xSemaphoreGive(mutexDatos);

            xSemaphoreGive(semUserRecibido);
        }
    }
    else if (strcmp(topic, T_RESP_PROD) == 0) {

        if (xSemaphoreTake(mutexDatos, pdMS_TO_TICKS(100)) == pdTRUE) {

            numProductos = 0;

            JsonArray arr = doc.as<JsonArray>();

            for (JsonObject p : arr) {

                if (numProductos >= MAX_PRODUCTOS) break;

                productos[numProductos].id     = p["id"]     | 0;
                productos[numProductos].precio = p["precio"] | 0L;
                productos[numProductos].stock  = p["stock"]  | 0;

                strlcpy(
                    productos[numProductos].nombre,
                    p["nombre"] | "Producto",
                    20
                );

                numProductos++;
            }

            xSemaphoreGive(mutexDatos);

            xSemaphoreGive(semProdRecibidos);
        }
    }
}

// ==========================================================
// SERVO
// ==========================================================
void activarServoProducto(int idx) {

    if (idx < 0 || idx >= MAX_PRODUCTOS) return;

    Serial.printf(
        "[SERVO] Activando canal %d (GPIO %d)\n",
        idx + 1,
        PINES_SERVOS[idx]
    );

    misServos[idx].write(180);

    vTaskDelay(pdMS_TO_TICKS(SERVO_DELAY_MS));

    misServos[idx].write(0);

    vTaskDelay(pdMS_TO_TICKS(500));
}

// ==========================================================
// TAREA VENDING
// ==========================================================
void taskVending(void* pv) {

    Serial.println("[SISTEMA] Tarea Vending Inicializada Correctamente.");

    for (;;) {

        if (!mostrarMensajeListo) {
            vTaskDelay(pdMS_TO_TICKS(200));
            continue;
        }

        if (!rfid.PICC_IsNewCardPresent() ||
            !rfid.PICC_ReadCardSerial()) {

            vTaskDelay(pdMS_TO_TICKS(100));
            continue;
        }

        uint32_t ahora = millis();

        bool mismoCard = true;

        for (int i = 0; i < 4; i++) {

            if (rfid.uid.uidByte[i] != ultimoUID[i]) {
                mismoCard = false;
                break;
            }
        }

        if (mismoCard &&
            (ahora - tsUltimaLectura < DEBOUNCE_RFID_MS)) {

            rfid.PICC_HaltA();
            rfid.PCD_StopCrypto1();

            vTaskDelay(pdMS_TO_TICKS(100));

            continue;
        }

        memcpy(ultimoUID, rfid.uid.uidByte, 4);

        tsUltimaLectura = ahora;

        char uid_str[10];

        snprintf(
            uid_str,
            sizeof(uid_str),
            "%02X%02X%02X%02X",
            rfid.uid.uidByte[0],
            rfid.uid.uidByte[1],
            rfid.uid.uidByte[2],
            rfid.uid.uidByte[3]
        );

        rfid.PICC_HaltA();
        rfid.PCD_StopCrypto1();

        Serial.println("\n========================================================");
        Serial.printf("[RFID] ¡TARJETA DETECTADA! UID: %s\n", uid_str);
        Serial.println("========================================================");

        lcdPrintLine(0, "Verificando...");

        xSemaphoreTake(semUserRecibido, 0);

        xSemaphoreTake(mutexMQTT, portMAX_DELAY);

        StaticJsonDocument<64> reqUser;

        reqUser["uid"] = uid_str;

        char bufUser[64];

        serializeJson(reqUser, bufUser);

        mqttClient.publish(T_REQ_USUARIO, bufUser);

        xSemaphoreGive(mutexMQTT);

        bool userOk =
            (xSemaphoreTake(
                semUserRecibido,
                pdMS_TO_TICKS(TIMEOUT_RESP_MS)
            ) == pdTRUE);

        int userId = 0;
        long creditos = 0;
        bool esNuevo = false;
        bool userValido = false;

        if (userOk) {

            xSemaphoreTake(mutexDatos, portMAX_DELAY);

            userId = datoUsuario.id;
            creditos = datoUsuario.creditos;
            esNuevo = datoUsuario.es_nuevo;
            userValido = datoUsuario.valido;

            xSemaphoreGive(mutexDatos);
        }

        if (!userOk || !userValido) {

            Serial.println("[ERR] Usuario no válido o problema de conexión.");

            lcdPrintLine(
                0,
                !userOk ? "Error de Red" : "Usuario Invalido"
            );

            vTaskDelay(pdMS_TO_TICKS(2000));

            lcdPrintLine(0, "Pase tarjeta");

            Serial.println("\n[SISTEMA] Listo. Pase tarjeta...");

            continue;
        }

        if (esNuevo) {

            lcdPrintLine(0, "¡Bienvenido!");

            char cBuf[16];

            snprintf(cBuf, sizeof(cBuf), "Reg:$%ld", creditos);

            lcdPrintLine(1, cBuf);

            vTaskDelay(pdMS_TO_TICKS(2000));
        }

        lcdPrintLine(0, "Cargando menu...");

        xSemaphoreTake(semProdRecibidos, 0);

        xSemaphoreTake(mutexMQTT, portMAX_DELAY);

        mqttClient.publish(T_REQ_PROD, "{}");

        xSemaphoreGive(mutexMQTT);

        bool prodOk =
            (xSemaphoreTake(
                semProdRecibidos,
                pdMS_TO_TICKS(TIMEOUT_RESP_MS)
            ) == pdTRUE);

        if (!prodOk || numProductos == 0) {

            lcdPrintLine(0, "Error Productos");

            vTaskDelay(pdMS_TO_TICKS(2000));

            lcdPrintLine(0, "Pase tarjeta");

            Serial.println("\n[SISTEMA] Listo. Pase tarjeta...");

            continue;
        }

        xSemaphoreTake(mutexDatos, portMAX_DELAY);

        int nProd = numProductos;

        ProductData prods[MAX_PRODUCTOS];

        for (int i = 0; i < nProd; i++) {
            prods[i] = productos[i];
        }

        xSemaphoreGive(mutexDatos);

        Serial.println("\n============= MENÚ DE OPCIONES DISPONIBLES =============");

        Serial.printf(
            "Usuario ID: %d | Saldo Disponible: $%ld\n",
            userId,
            creditos
        );

        for (int i = 0; i < nProd; i++) {

            Serial.printf(
                "  [%d] %s — Precio: $%ld (Stock: %d)\n",
                i + 1,
                prods[i].nombre,
                prods[i].precio,
                prods[i].stock
            );
        }

        Serial.println("========================================================");

        char topBuf[17];

        snprintf(topBuf, sizeof(topBuf), "Saldo:$%ld", creditos);

        lcdPrintLine(0, topBuf);

        int productoVisible = 0;

        uint32_t tsUltimoProducto = 0;

        char tecla = 0;

        uint32_t tStart = millis();

        // ==========================================================
        // CORRECCIÓN MQTT
        // ==========================================================
        while ((millis() - tStart) < TIMEOUT_TECLA_MS) {

            // Mantener viva conexión MQTT
            xSemaphoreTake(mutexMQTT, portMAX_DELAY);
            mqttClient.loop();
            xSemaphoreGive(mutexMQTT);

            tecla = teclado.getKey();

            if (tecla) break;

            if (millis() - tsUltimoProducto >= ROTATE_PRODUCTO_MS ||
                tsUltimoProducto == 0) {

                char bottomBuf[17];

                snprintf(
                    bottomBuf,
                    sizeof(bottomBuf),
                    "%d:%s $%ld",
                    productoVisible + 1,
                    prods[productoVisible].nombre,
                    prods[productoVisible].precio
                );

                lcdPrintLine(1, bottomBuf);

                productoVisible =
                    (productoVisible + 1) % nProd;

                tsUltimoProducto = millis();
            }

            vTaskDelay(pdMS_TO_TICKS(20));
        }

        if (!tecla || tecla == '0') {

            Serial.println(
                "\n[SISTEMA] Cancelado: Tiempo agotado."
            );

            lcdPrintLine(0, "Operacion Cancel");

            vTaskDelay(pdMS_TO_TICKS(1500));

            lcdPrintLine(0, "Pase tarjeta");

            Serial.println("\n[SISTEMA] Listo. Pase tarjeta...");

            continue;
        }

        Serial.printf(
            "\n[TECLADO] Opción seleccionada: [%c]\n",
            tecla
        );

        int pIdx = tecla - '1';

        if (pIdx < 0 ||
            pIdx >= nProd ||
            prods[pIdx].stock <= 0 ||
            creditos < prods[pIdx].precio) {

            if (pIdx < 0 || pIdx >= nProd) {

                lcdPrintLine(0, "Opcion Invalida");
            }
            else if (prods[pIdx].stock <= 0) {

                lcdPrintLine(0, "Sin Stock");
            }
            else {

                lcdPrintLine(0, "Saldo Insufic.");
            }

            vTaskDelay(pdMS_TO_TICKS(2000));

            lcdPrintLine(0, "Pase tarjeta");

            continue;
        }

        long nuevoCredito =
            creditos - prods[pIdx].precio;

        int nuevoStock =
            prods[pIdx].stock - 1;

        Serial.printf(
            "[EXITO] Despachando: %s\n",
            prods[pIdx].nombre
        );

        lcdPrintLine(0, "Despachando...");
        lcdPrintLine(1, prods[pIdx].nombre);

        activarServoProducto(pIdx);

        lcdPrintLine(0, "¡Exito! Saldo:");

        char sBuf[16];

        snprintf(sBuf, sizeof(sBuf), "$%ld", nuevoCredito);

        lcdPrintLine(1, sBuf);

        VendingEvent ev;

        strlcpy(ev.rfid_uid, uid_str, sizeof(ev.rfid_uid));

        ev.usuario_id = userId;
        ev.producto_id = prods[pIdx].id;
        ev.creditos_usados = prods[pIdx].precio;
        ev.stock_restante = nuevoStock;
        ev.saldo_nuevo = nuevoCredito;

        xQueueSend(logQueue, &ev, portMAX_DELAY);

        vTaskDelay(pdMS_TO_TICKS(2500));

        lcdPrintLine(0, "Pase tarjeta");

        Serial.println("\n[SISTEMA] Listo. Pase tarjeta...");
    }
}

// ==========================================================
// PUBLICAR LOTE
// ==========================================================
bool PUBLICAR_LOTE_VENTAS(
    VendingEvent eventos[],
    int cantidad
) {

    if (cantidad <= 0) return true;

    StaticJsonDocument<1536> doc;

    JsonArray arr = doc.to<JsonArray>();

    for (int i = 0; i < cantidad; i++) {

        JsonObject item = arr.createNestedObject();

        item["rfid_uid"]        = eventos[i].rfid_uid;
        item["usuario_id"]      = eventos[i].usuario_id;
        item["producto_id"]     = eventos[i].producto_id;
        item["creditos_usados"] = eventos[i].creditos_usados;
        item["stock_restante"]  = eventos[i].stock_restante;
        item["saldo_nuevo"]     = eventos[i].saldo_nuevo;
    }

    char buf[1536];

    size_t len = serializeJson(doc, buf, sizeof(buf));

    if (len == 0) return false;

    xSemaphoreTake(mutexMQTT, portMAX_DELAY);

    bool ok = mqttClient.publish(T_EVENTOS, buf);

    xSemaphoreGive(mutexMQTT);

    return ok;
}

// ==========================================================
// TASK MQTT
// ==========================================================
void taskMQTT(void* pv) {

    VendingEvent ev;

    VendingEvent lote[BULK_EVENTOS_MAX];

    int loteCount = 0;

    uint32_t tsPrimerPendiente = 0;

    for (;;) {

        xSemaphoreTake(mutexMQTT, portMAX_DELAY);

        if (!mqttClient.connected()) {

            xSemaphoreGive(mutexMQTT);

            reconnectMQTT();
        }
        else {

            mqttClient.loop();

            xSemaphoreGive(mutexMQTT);
        }

        while (
            loteCount < BULK_EVENTOS_MAX &&
            xQueueReceive(logQueue, &ev, 0) == pdTRUE
        ) {

            lote[loteCount++] = ev;

            if (loteCount == 1)
                tsPrimerPendiente = millis();
        }

        if (
            loteCount >= BULK_EVENTOS_MAX ||
            (
                loteCount > 0 &&
                (millis() - tsPrimerPendiente) >= BULK_FLUSH_MS
            )
        ) {

            if (PUBLICAR_LOTE_VENTAS(lote, loteCount)) {

                loteCount = 0;

                tsPrimerPendiente = 0;
            }
        }

        vTaskDelay(pdMS_TO_TICKS(50));
    }
}

// ==========================================================
// SETUP
// ==========================================================
void setup() {

    Serial.begin(115200);

    delay(10);

    Serial.println("\n--- [SISTEMA SMART-VENDING ESP32] ---");

    for (int i = 0; i < 4; i++) {

        misServos[i].attach(PINES_SERVOS[i]);

        misServos[i].write(0);
    }

    Wire.begin(21, 22);

    lcd.init();

    lcd.backlight();

    lcdPrintLine(0, "Iniciando...");

    SPI.begin(18, 19, 23, SS_PIN);

    rfid.PCD_Init();

    logQueue = xQueueCreate(10, sizeof(VendingEvent));

    semUserRecibido =
        xSemaphoreCreateCounting(5, 0);

    semProdRecibidos =
        xSemaphoreCreateCounting(5, 0);

    mutexMQTT = xSemaphoreCreateMutex();

    mutexDatos = xSemaphoreCreateMutex();

    Serial.printf(
        "[WIFI] Conectando a SSID: %s\n",
        WIFI_SSID
    );

    WiFi.begin(WIFI_SSID, WIFI_PASS);

    while (WiFi.status() != WL_CONNECTED) {

        delay(500);

        Serial.print(".");
    }

    Serial.printf(
        "\n[WIFI] Conectado. IP: %s\n",
        WiFi.localIP().toString().c_str()
    );

    mqttClient.setServer(MQTT_SERVER, MQTT_PORT);

    mqttClient.setCallback(mqttCallback);

    mqttClient.setBufferSize(2048);

    // ==========================================================
    // NUEVO KEEPALIVE
    // ==========================================================
    mqttClient.setKeepAlive(120);

    xTaskCreatePinnedToCore(
        taskMQTT,
        "MQTT",
        4096,
        NULL,
        1,
        NULL,
        0
    );

    xTaskCreatePinnedToCore(
        taskVending,
        "Vending",
        8192,
        NULL,
        2,
        NULL,
        1
    );
}

void loop() {}

// ==========================================================
// RECONNECT MQTT
// ==========================================================
void reconnectMQTT() {

    while (!mqttClient.connected()) {

        Serial.printf(
            "[MQTT] Conectando al broker: %s...\n",
            MQTT_SERVER
        );

        String clientId =
            "ESP32Vending-" + String(random(0, 9999));

        if (
            mqttClient.connect(
                clientId.c_str(),
                MQTT_USER,
                MQTT_PASS
            )
        ) {

            Serial.println(
                "[MQTT] ¡Enlazado y Suscrito exitosamente!"
            );

            mqttClient.subscribe(T_RESP_USUARIO);

            mqttClient.subscribe(T_RESP_PROD);

            if (!mostrarMensajeListo) {

                mostrarMensajeListo = true;

                lcdPrintLine(0, "Pase tarjeta");

                Serial.println(
                    "\n[SISTEMA] Máquina lista. Pase tarjeta..."
                );
            }
        }
        else {

            Serial.printf(
                "[MQTT] Error rc=%d. Reintentando...\n",
                mqttClient.state()
            );

            delay(3000);
        }
    }
}

// ==========================================================
// LCD
// ==========================================================
void lcdPrintLine(int row, const char* text) {

    lcd.setCursor(0, row);

    lcd.print("                ");

    lcd.setCursor(0, row);

    lcd.print(text);
}