package main

import (
	"bufio"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/goccy/go-yaml"
	"github.com/juju/loggo"
	"github.com/juju/loggo/loggocolor"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	config = map[string]string{}
	logger = loggo.GetLogger("")

	enecTemperature = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "enecsys_temperature",
		Help: "Temperature of the solar panel.",
	},
		[]string{"id"},
	)
	enecWh = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "enecsys_watthours_today",
		Help: "Watt hours produced today.",
	},
		[]string{"id"},
	)
	enecKwh = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "enecsys_kilowatthours_history",
		Help: "Watt hours produced in history.",
	},
		[]string{"id"},
	)
	enecLifekwh = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "enecsys_kilowatthours_total",
		Help: "Watt hours produced in total.",
	},
		[]string{"id"},
	)
	enecTime1 = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "enecsys_time1",
		Help: "Time 1.",
	},
		[]string{"id"},
	)
	enecTime2 = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "enecsys_time2",
		Help: "Time 2.",
	},
		[]string{"id"},
	)
	enecDcpower = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "enecsys_dc_power",
		Help: "DC power.",
	},
		[]string{"id"},
	)
	enecDcvolt = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "enecsys_dc_volt",
		Help: "DC voltage.",
	},
		[]string{"id"},
	)
	enecDccurrent = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "enecsys_dc_current",
		Help: "DC current.",
	},
		[]string{"id"},
	)
	enecEfficiency = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "enecsys_efficiency",
		Help: "Inverter efficiency.",
	},
		[]string{"id"},
	)
	enecAcpower = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "enecsys_ac_power",
		Help: "AC power.",
	},
		[]string{"id"},
	)
	enecAcvolt = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "enecsys_ac_volt",
		Help: "AC voltage.",
	},
		[]string{"id"},
	)
	enecAccurrent = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "enecsys_ac_current",
		Help: "AC current.",
	},
		[]string{"id"},
	)
	enecAcfreq = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "enecsys_ac_frequency",
		Help: "AC frequency.",
	},
		[]string{"id"},
	)
)

func init() {

	loggo.ConfigureLoggers("<root>=ERROR")
	loggo.ReplaceDefaultWriter(loggocolor.NewColorWriter(os.Stderr))

	// Metrics have to be registered to be exposed:
	prometheus.MustRegister(enecTemperature)
	prometheus.MustRegister(enecWh)
	prometheus.MustRegister(enecKwh)
	prometheus.MustRegister(enecLifekwh)
	prometheus.MustRegister(enecTime1)
	prometheus.MustRegister(enecTime2)
	prometheus.MustRegister(enecDcpower)
	prometheus.MustRegister(enecDcvolt)
	prometheus.MustRegister(enecDccurrent)
	prometheus.MustRegister(enecEfficiency)
	prometheus.MustRegister(enecAcpower)
	prometheus.MustRegister(enecAcvolt)
	prometheus.MustRegister(enecAccurrent)
	prometheus.MustRegister(enecAcfreq)
}

func getCredentials(credentialsFile string) {

	osFile, err := os.Open(credentialsFile)
	if err != nil {
		logger.Infof(fmt.Sprintf("Couldn't read credentials file: %s", err.Error()))
	}

	err = yaml.NewDecoder(osFile).Decode(&config)

	config["mqtt"] = "ok"

	if err != nil {
		logger.Errorf(fmt.Sprintf("Couldn't parse config file: %s", err.Error()))
		config["mqtt"] = "impossible"
	}

	_, ok := config["userName"]
	if !ok {
		logger.Errorf("userName missing.")
		config["mqtt"] = "impossible"
	}
	_, ok = config["password"]
	if !ok {
		logger.Errorf("password missing.")
		config["mqtt"] = "impossible"
	}
	_, ok = config["mqttAddress"]
	if !ok {
		logger.Errorf("mqttAddress missing.")
		config["mqtt"] = "impossible"
	}
	_, ok = config["clientName"]
	if !ok {
		logger.Errorf("clientName missing.")
		config["mqtt"] = "impossible"
	}
	if config["mqtt"] != "ok" {
		logger.Errorf("YAML file needs to have this structure:\n\n---\nuserName: valUserName\npassword: valPassword\nmqttAddress: \"tcp://host:1883\"\nclientName: valClientName\n\nNo MQTT publishing will be active")
	} else {
		logger.Errorf("MQTT publishing active!")
	}
}

func publishMqtt(topic string, value string) {
	if config["mqtt"] == "ok" {

		mqtt.ERROR = log.New(os.Stdout, "", 0)
		opts := mqtt.NewClientOptions().AddBroker(config["nmqttAddress"]).SetClientID(config["clientName"])
		opts.SetUsername(config["userName"])
		opts.SetPassword(config["password"])
		opts.SetKeepAlive(2 * time.Second)
		opts.SetPingTimeout(1 * time.Second)

		client := mqtt.NewClient(opts)
		if token := client.Connect(); token.Wait() && token.Error() != nil {
			fmt.Printf("Connection to broker failed: %s", token.Error())
		} else {
			fmt.Printf("publishMqtt: pushing to %s value: %s\n", topic, value)
			token := client.Publish(topic, 0, true, value)
			token.Wait()

			client.Disconnect(250)
		}
	}
}

func main() {

	if len(os.Args) > 1 {
		getCredentials(os.Args[1])
	} else {
		logger.Errorf(fmt.Sprintf("If you want MQTT logging, add path to configuration file as first argument to program: %s /path/to/config_file", os.Args[0]))
		getCredentials("undefined_path_and_file")
	}

	fmt.Println("\nLogging level:")
	fmt.Println(loggo.LoggerInfo())
	fmt.Println("")

	listener, err := net.Listen("tcp", "0.0.0.0:5040")
	if err != nil {
		fmt.Println("tcp server listener error:", err)
	} else {
		fmt.Println("listening...")
	}

	http.Handle("/metrics", promhttp.Handler())
	go http.ListenAndServe(":5041", nil)

	// Endless listener for TCP connections
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("tcp server accept error", err)
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	// Test with cat raw.txt | while read line; do echo $line; printf "$line\15" | nc -c 127.0.0.1 5040; done
	bufferBytes, err := bufio.NewReader(conn).ReadBytes(0x0D)

	if err != nil {
		conn.Close()
		return
	}

	message := string(bufferBytes)
	// Remove trailing \m
	message = message[:len(message)-1]

	if len(message) == 77 {
		fmt.Println(message, "length:", len(message))
		code := message[18:20]
		if code == "WS" {
			fmt.Println("Code:", code)
			data := message[21:]

			p, err := base64.RawURLEncoding.DecodeString(data)
			if err != nil {
				// handle error
			}
			hexzigbee := hex.EncodeToString(p)
			fmt.Println("hex:", hexzigbee, "length:", len(hexzigbee))

			hexid := hexzigbee[0:8]
			fmt.Println("HexID:", hexid)

			baseTopic := "enecsys/" + hexid + "/"

			data = hexzigbee[64:66]
			dec, err := strconv.ParseUint(data, 16, 32)
			temperature := float64(dec)
			fmt.Println("Temperature:", temperature)
			enecTemperature.WithLabelValues(hexid).Set(temperature)
			topic := baseTopic + "temperature"
			publishMqtt(topic, strconv.FormatFloat(temperature, 'f', 1, 64))

			data = hexzigbee[66:70]
			dec, err = strconv.ParseUint(data, 16, 32)
			wh := float64(dec)
			fmt.Println("Wh:", wh)
			enecWh.WithLabelValues(hexid).Set(wh)
			topic = baseTopic + "wh"
			publishMqtt(topic, strconv.FormatFloat(wh, 'f', 1, 64))

			data = hexzigbee[70:74]
			dec, err = strconv.ParseUint(data, 16, 32)
			kwh := float64(dec)
			fmt.Println("kWh:", kwh)
			enecKwh.WithLabelValues(hexid).Set(kwh)
			topic = baseTopic + "kwh"
			publishMqtt(topic, strconv.FormatFloat(kwh, 'f', 1, 64))

			lifewh := 1000*kwh + wh
			lifekwh := kwh + 0.001*wh
			fmt.Println("life_kWh:", lifekwh)
			enecLifekwh.WithLabelValues(hexid).Set(lifekwh)
			topic = baseTopic + "lifeWh"
			publishMqtt(topic, strconv.FormatFloat(lifewh, 'f', 1, 64))

			data = hexzigbee[18:22]
			dec, err = strconv.ParseUint(data, 16, 32)
			time1 := float64(dec)
			fmt.Println("Time 1:", time1)
			enecTime1.WithLabelValues(hexid).Set(time1)
			topic = baseTopic + "time1"
			publishMqtt(topic, strconv.FormatFloat(time1, 'f', 1, 64))

			data = hexzigbee[30:36]
			dec, err = strconv.ParseUint(data, 16, 32)
			time2 := float64(dec)
			fmt.Println("Time 2:", time2)
			enecTime2.WithLabelValues(hexid).Set(time2)
			topic = baseTopic + "time2"
			publishMqtt(topic, strconv.FormatFloat(time2, 'f', 1, 64))

			data = hexzigbee[50:54]
			dec, err = strconv.ParseUint(data, 16, 32)
			dcpower := float64(dec)
			fmt.Println("DCPower:", dcpower)
			enecDcpower.WithLabelValues(hexid).Set(dcpower)
			topic = baseTopic + "dcpower"
			publishMqtt(topic, strconv.FormatFloat(dcpower, 'f', 1, 64))

			data = hexzigbee[46:50]
			dec, err = strconv.ParseUint(data, 16, 32)
			dccurrent := 0.025 * float64(dec)

			dcvolt := dcpower / dccurrent
			fmt.Println("DCVolt:", dcvolt)
			enecDcvolt.WithLabelValues(hexid).Set(dcvolt)
			topic = baseTopic + "dcvolt"
			publishMqtt(topic, strconv.FormatFloat(dcvolt, 'f', 1, 64))

			fmt.Println("DCCurrent:", dccurrent)
			enecDccurrent.WithLabelValues(hexid).Set(dccurrent)
			topic = baseTopic + "dccurrent"
			publishMqtt(topic, strconv.FormatFloat(dccurrent, 'f', 1, 64))

			data = hexzigbee[54:58]
			dec, err = strconv.ParseUint(data, 16, 32)
			efficiency := 0.1 * float64(dec)
			fmt.Println("Efficiency:", efficiency)
			enecEfficiency.WithLabelValues(hexid).Set(efficiency)
			topic = baseTopic + "efficiency"
			publishMqtt(topic, strconv.FormatFloat(efficiency, 'f', 1, 64))

			acpower := dcpower * efficiency / 100
			fmt.Println("ACPower:", acpower)
			enecAcpower.WithLabelValues(hexid).Set(acpower)
			topic = baseTopic + "acpower"
			publishMqtt(topic, strconv.FormatFloat(acpower, 'f', 1, 64))

			data = hexzigbee[60:64]
			dec, err = strconv.ParseUint(data, 16, 32)
			acvolt := float64(dec)
			fmt.Println("ACVolt:", acvolt)
			enecAcvolt.WithLabelValues(hexid).Set(acvolt)
			topic = baseTopic + "acvolt"
			publishMqtt(topic, strconv.FormatFloat(acvolt, 'f', 1, 64))

			accurrent := acpower / acvolt
			fmt.Println("ACCurrent:", accurrent)
			enecAccurrent.WithLabelValues(hexid).Set(accurrent)
			topic = baseTopic + "accurrent"
			publishMqtt(topic, strconv.FormatFloat(accurrent, 'f', 1, 64))

			data = hexzigbee[58:60]
			dec, err = strconv.ParseUint(data, 16, 32)
			acfreq := float64(dec)
			fmt.Println("ACFreq:", acfreq)
			enecAcfreq.WithLabelValues(hexid).Set(acfreq)
			topic = baseTopic + "acfreq"
			publishMqtt(topic, strconv.FormatFloat(acfreq, 'f', 1, 64))

		}
	}

	handleConnection(conn)
}
