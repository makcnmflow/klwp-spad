pub fn url_decode(input: &str) -> String {
    let mut decoded = String::new();
    let mut chars = input.chars();
    while let Some(c) = chars.next() {
        if c == '%' {
            let mut hex = String::new();
            if let Some(h1) = chars.next() { hex.push(h1); }
            if let Some(h2) = chars.next() { hex.push(h2); }
            if let Ok(val) = u8::from_str_radix(&hex, 16) {
                decoded.push(val as char);
            } else {
                decoded.push('%');
                decoded.push_str(&hex);
            }
        } else if c == '+' {
            decoded.push(' ');
        } else {
            decoded.push(c);
        }
    }
    decoded
}

pub fn parse_soundpad_protocol(protocol_url: &str) -> Option<String> {
    let stripped = protocol_url.strip_prefix("soundpad://sound/url/")?;
    Some(stripped.to_string())
}

pub fn parse_voicemod_protocol(protocol_url: &str) -> Option<String> {
    if let Some(pos) = protocol_url.find("meme_code=") {
        let uuid = &protocol_url[pos + 10..];
        let clean_uuid: String = uuid.chars().filter(|c| c.is_alphanumeric() || *c == '-').collect();
        if !clean_uuid.is_empty() {
            return Some(clean_uuid);
        }
    }
    None
}

// I spent literally 7 hours of my life fighting with C# and windows I hate microsoft
pub fn set_default_windows_microphone(device_name: &str) {
    if device_name.is_empty() {
        return;
    }

    let escaped_name = device_name.replace("'", "''");

    let script_template = r##"
$code = @'
using System;
using System.Runtime.InteropServices;

public enum ERole : uint {
    eConsole = 0,
    eMultimedia = 1,
    eCommunications = 2,
    ERole_enum_count = 3
}

[ComImport]
[Guid("F8679F50-850A-41CF-9C72-430F290290C8")]
[InterfaceType(ComInterfaceType.InterfaceIsIUnknown)]
internal interface IPolicyConfig {
    int GetMixFormat();
    int GetDeviceFormat();
    int ResetDeviceFormat();
    int SetDeviceFormat();
    int GetProcessingPeriod();
    int SetProcessingPeriod();
    int GetShareMode();
    int SetShareMode();
    int GetPropertyValue();
    int SetPropertyValue();
    [PreserveSig]
    int SetDefaultEndpoint([In] [MarshalAs(UnmanagedType.LPWStr)] string wszDeviceId, [In] [MarshalAs(UnmanagedType.U4)] ERole role);
    int SetEndpointVisibility();
}

[ComImport, Guid("870AF99C-171D-4F9E-AF0D-E63DF40C2BC9")]
internal class CPolicyConfigClient {}

[ComImport]
[Guid("D666063F-1587-4E43-81F1-B948E807363F")]
[InterfaceType(ComInterfaceType.InterfaceIsIUnknown)]
internal interface IMMDevice {
    int Activate(ref Guid iid, uint dwClsCtx, IntPtr pActivationParams, out IntPtr ppInterface);
    [PreserveSig]
    int OpenPropertyStore(uint stgAccess, out IntPtr ppProperties);
    [PreserveSig]
    int GetId([MarshalAs(UnmanagedType.LPWStr)] out string ppstrId);
    int GetState(out uint pdwState);
}

[ComImport]
[Guid("0BD7A1BE-7A1A-44DB-8397-CC5392387B5E")]
[InterfaceType(ComInterfaceType.InterfaceIsIUnknown)]
internal interface IMMDeviceCollection {
    [PreserveSig]
    int GetCount(out uint pcDevices);
    [PreserveSig]
    int Item(uint nDevice, out IMMDevice ppDevice);
}

[ComImport]
[Guid("A95664D2-9614-4F35-A746-DE8DB63617E6")]
[InterfaceType(ComInterfaceType.InterfaceIsIUnknown)]
internal interface IMMDeviceEnumerator {
    [PreserveSig]
    int EnumAudioEndpoints(int dataFlow, uint dwStateMask, out IMMDeviceCollection ppDevices);
    [PreserveSig]
    int GetDefaultAudioEndpoint(int dataFlow, int role, out IMMDevice ppEndpoint);
    int GetDevice([MarshalAs(UnmanagedType.LPWStr)] string wszDeviceId, out IMMDevice ppDevice);
    int RegisterEndpointNotificationCallback(IntPtr pClient);
    int UnregisterEndpointNotificationCallback(IntPtr pClient);
}

[ComImport, Guid("BCDE0395-E52F-467C-8E3D-C4579291692E")]
internal class MMDeviceEnumeratorClient {}

[ComImport]
[Guid("886D8EEB-8CF2-4446-8D02-CDBA1DBDCF99")]
[InterfaceType(ComInterfaceType.InterfaceIsIUnknown)]
internal interface IPropertyStore {
    [PreserveSig]
    int GetCount(out uint cProps);
    [PreserveSig]
    int GetAt(uint iProp, out IntPtr pkey);
    [PreserveSig]
    int GetValue(ref PROPERTYKEY key, [In, Out] PROPVARIANT pv);
}

[StructLayout(LayoutKind.Sequential)]
internal struct PROPERTYKEY {
    public Guid fmtid;
    public uint pid;
}

[StructLayout(LayoutKind.Explicit)]
internal class PROPVARIANT {
    [FieldOffset(0)] public ushort vt;
    [FieldOffset(8)] public IntPtr ptr;
}

public class AudioChanger {
    public static void SetDefaultDevice(string friendlyName) {
        try {
            Type enumeratorType = Type.GetTypeFromCLSID(new Guid("BCDE0395-E52F-467C-8E3D-C4579291692E"));
            IMMDeviceEnumerator enumerator = (IMMDeviceEnumerator)Activator.CreateInstance(enumeratorType);

            IMMDeviceCollection collection;
            enumerator.EnumAudioEndpoints(1, 1, out collection);

            uint count;
            collection.GetCount(out count);

            for (uint i = 0; i < count; i++) {
                IMMDevice device;
                collection.Item(i, out device);

                IntPtr propStorePtr;
                device.OpenPropertyStore(0, out propStorePtr);
                IPropertyStore propStore = (IPropertyStore)Marshal.GetObjectForIUnknown(propStorePtr);

                PROPERTYKEY key = new PROPERTYKEY();
                key.fmtid = new Guid("a45c254e-df1c-4efd-8020-67d146a850e0");
                key.pid = 14;

                PROPVARIANT pv = new PROPVARIANT();
                propStore.GetValue(ref key, pv);

                string name = Marshal.PtrToStringUni(pv.ptr);

                if (name != null && name.IndexOf(friendlyName, StringComparison.OrdinalIgnoreCase) >= 0) {
                    string deviceId;
                    device.GetId(out deviceId);

                    Type policyConfigType = Type.GetTypeFromCLSID(new Guid("870AF99C-171D-4F9E-AF0D-E63DF40C2BC9"));
                    IPolicyConfig policyConfig = (IPolicyConfig)Activator.CreateInstance(policyConfigType);

                    policyConfig.SetDefaultEndpoint(deviceId, ERole.eConsole);
                    policyConfig.SetDefaultEndpoint(deviceId, ERole.eMultimedia);
                    policyConfig.SetDefaultEndpoint(deviceId, ERole.eCommunications);
                    break;
                }
            }
        } catch (Exception) {
        }
    }
}
'@
Add-Type -TypeDefinition $code -ErrorAction SilentlyContinue
[AudioChanger]::SetDefaultDevice('__DEVICE_NAME__')
"##;

    let script = script_template.replace("__DEVICE_NAME__", &escaped_name);

    let _ = std::process::Command::new("powershell")
        .args(&[
            "-NoProfile",
            "-ExecutionPolicy", "Bypass",
            "-WindowStyle", "Hidden",
            "-Command", &script
        ])
        .output();
}

pub fn register_custom_protocol() -> Result<(), Box<dyn std::error::Error>> {
    let exe_path = std::env::current_exe()?;
    let exe_str = exe_path.display().to_string().replace("\\", "\\\\");

    let script = format!(
        r#"
$protocols = @("soundpad", "voicemod")
foreach ($p in $protocols) {{
    $registryPath = "HKCU:\Software\Classes\$p"
    if (-not (Test-Path $registryPath)) {{
        New-Item -Path $registryPath -Force | Out-Null
    }}
    Set-ItemProperty -Path $registryPath -Name "(Default)" -Value "URL:$p Protocol" -Force | Out-Null
    Set-ItemProperty -Path $registryPath -Name "URL Protocol" -Value "" -Force | Out-Null

    $iconPath = "$registryPath\DefaultIcon"
    if (-not (Test-Path $iconPath)) {{
        New-Item -Path $iconPath -Force | Out-Null
    }}
    Set-ItemProperty -Path $iconPath -Name "(Default)" -Value '"{}"' -Force | Out-Null

    $commandPath = "$registryPath\shell\open\command"
    if (-not (Test-Path $commandPath)) {{
        New-Item -Path $commandPath -Force | Out-Null
    }}
    Set-ItemProperty -Path $commandPath -Name "(Default)" -Value '"{}" "%1"' -Force | Out-Null
}}
"#,
        exe_str, exe_str
    );

    let _ = std::process::Command::new("powershell")
        .args(&[
            "-NoProfile",
            "-ExecutionPolicy", "Bypass",
            "-WindowStyle", "Hidden",
            "-Command", &script
        ])
        .output();

    Ok(())
}