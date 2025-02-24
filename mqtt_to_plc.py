import json
import paho.mqtt.client as mqtt
from pylogix import PLC

# MQTT Broker Settings
MQTT_BROKER = '192.168.60.150'  # Update with your MQTT broker's IP
MQTT_TOPIC = 'plc/lamp_control'

def on_connect(client, userdata, flags, rc):
    print("Connected to MQTT broker with result code:", rc)
    client.subscribe(MQTT_TOPIC)

def on_message(client, userdata, msg):
    try:
        payload_str = msg.payload.decode('utf-8')
        print("Received message:", payload_str)
        data = json.loads(payload_str)
    except Exception as e:
        print("Error parsing JSON payload:", e)
        return

    # Ensure the payload contains the required keys.
    if "PLC_IP" not in data or "LAMP_STATES" not in data:
        print("Invalid message format. Missing 'PLC_IP' or 'LAMP_STATES'.")
        return

    plc_ip = data["PLC_IP"]
    lamp_states = data["LAMP_STATES"]

    print(f"Updating PLC at {plc_ip} with lamp states: {lamp_states}")
    try:
        with PLC() as comm:
            comm.IPAddress = plc_ip
            for tag, state in lamp_states.items():
                result = comm.Write(tag, state)
                print(f"Wrote {tag} = {state} to PLC {plc_ip} (Result: {result})")
    except Exception as e:
        print("Error updating PLC:", e)

# Set up the MQTT client
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

print("Connecting to MQTT broker...")
client.connect(MQTT_BROKER, 1883, 60)

print("MQTT client is running and waiting for messages...")
client.loop_forever()

