import serial
import serial.tools.list_ports
import time

# Test ZPL command (prints "Hello RS232!")
test_zpl = "^XA^FO50,50^ADN,36,20^FDHello RS232!^FS^XZ"

def try_port(port_name, baudrate=9600):
    try:
        print(f"Trying {port_name} at {baudrate} baud...")
        ser = serial.Serial(
            port=port_name,
            baudrate=baudrate,
            bytesize=8,
            parity="N",
            stopbits=1,
            timeout=1,
            xonxoff=False,  # software flow control
            rtscts=False    # hardware flow control
        )
        ser.write(test_zpl.encode("utf-8"))
        time.sleep(1)
        ser.close()
        print(f"✅ Sent test label to {port_name}")
        return True
    except Exception as e:
        print(f"❌ Failed on {port_name}: {e}")
        return False

def main():
    ports = serial.tools.list_ports.comports()
    if not ports:
        print("No COM ports found!")
        return
    
    for port in ports:
        print(f"Detected: {port.device} - {port.description}")
        # Try common baud rates (9600, 19200, 115200)
        for baud in [9600, 19200, 115200]:
            if try_port(port.device, baud):
                print(f"🎉 Printer likely found on {port.device} at {baud} baud")
                return  # stop after first success
    
    print("⚠️ No response from any port. Check printer serial settings!")

if __name__ == "__main__":
    main()
