using System;
using System.Collections.Concurrent;
using System.Timers;
using Skyline.DataMiner.Analytics.GenericInterface;
using Skyline.DataMiner.Net;
using Skyline.DataMiner.Net.Messages;
using SLDataGateway.Caching;

[GQIMetaData(Name = "ParameterKeyToName")]
public class ParameterKeyToName : IGQIRowOperator, IGQIInputArguments, IGQIColumnOperator, IGQIOnInit
{
    private static readonly ProtocolCache _protocolCache = new ProtocolCache();

    private readonly GQIColumnDropdownArgument _parameterKeyColumnArg = new GQIColumnDropdownArgument("Parameter Key Column") { IsRequired = true };
    private readonly GQIColumnDropdownArgument _protocolColumnArg = new GQIColumnDropdownArgument("Protocol Name") { IsRequired = false };
    private readonly GQIColumnDropdownArgument _protocolVersionColumnArg = new GQIColumnDropdownArgument("Protocol Version") { IsRequired = false };
    private readonly GQIStringColumn _parameterNameColumn = new GQIStringColumn("Parameter Name");

    private IConnection _conn;

    private GQIColumn _parameterKeyColumn;
    private GQIColumn _protocolColumn;
    private GQIColumn _protocolVersionColumn;

    private GQIDMS _dms;

    public GQIArgument[] GetInputArguments()
    {
        return new GQIArgument[] { _parameterKeyColumnArg, _protocolColumnArg, _protocolVersionColumnArg };
    }

    public OnArgumentsProcessedOutputArgs OnArgumentsProcessed(OnArgumentsProcessedInputArgs args)
    {
        _parameterKeyColumn = args.GetArgumentValue(_parameterKeyColumnArg);
        _protocolColumn = args.GetArgumentValue(_protocolColumnArg);
        _protocolVersionColumn = args.GetArgumentValue(_protocolVersionColumnArg);

        return new OnArgumentsProcessedOutputArgs();
    }

    public void HandleColumns(GQIEditableHeader header)
    {
        header.AddColumns(_parameterNameColumn);
    }

    public OnInitOutputArgs OnInit(OnInitInputArgs args)
    {
        try
        {
            _dms = args.DMS;
            if (_dms != null)
            {
                _conn = _dms.GetConnection();
            }
        }
        catch (Exception)
        {
        }

        return new OnInitOutputArgs();
    }

    public void HandleRow(GQIEditableRow row)
    {
        try
        {
            // Init name with Key
            String parameterKey = row.GetValue<String>(_parameterKeyColumn);
            ParamID paramID = ParamID.FromString(parameterKey);

            // Find the correct protocol, based on the columns that have been linked this can be either a raw protocol (better performance/caching) or an element protocol
            GetProtocolInfoResponseMessage protocolInfo = null;
            if (_protocolColumn != null && _protocolVersionColumn != null)
            {// by protocol
                String protocol = row.GetValue<String>(_protocolColumn);
                String version = row.GetValue<String>(_protocolVersionColumn);

                if (!_protocolCache.TryGet($"{protocol}/{version}", out protocolInfo))
                {
                    protocolInfo = _conn.GetProtocol(protocol, version);
                    _protocolCache.Cache($"{protocol}/{version}", protocolInfo);
                }
            }
            else if (!_protocolCache.TryGet($"{paramID.DataMinerID}/{paramID.EID}", out protocolInfo))
            {// by element
                protocolInfo = _conn.GetElementProtocol(paramID.DataMinerID, paramID.EID);
                _protocolCache.Cache($"{paramID.DataMinerID}/{paramID.EID}", protocolInfo);
            }

            // Once we have the protocol, lookup the map IDToName
            String parameterName = parameterKey;
            if (protocolInfo != null)
            {
                parameterName = protocolInfo.GetParameterName(paramID.PID);
            }

            row.SetValue<String>(_parameterNameColumn, parameterName);
        }
        catch (Exception)
		{
			row.SetValue<String>(_parameterNameColumn, "<Something went wrong>");
		}
    }
}

public class ProtocolCache
{
    private const double ClearInterval = 60 * 60 * 1000; // 1 hour in milliseconds
    private readonly ConcurrentDictionary<String, GetProtocolInfoResponseMessage> _map = new ConcurrentDictionary<String, GetProtocolInfoResponseMessage>();
    private readonly Timer clearTimer = new Timer(ClearInterval);

    public ProtocolCache()
    {
        clearTimer.Elapsed += OnClearTimerElapsed;
        clearTimer.AutoReset = false; // Ensure the timer runs only once unless reset
        clearTimer.Start();
    }

    public void Cache(String key, GetProtocolInfoResponseMessage protocol)
    {
        ResetTimer();

        _map[key] = protocol;
    }

    public bool TryGet(String key, out GetProtocolInfoResponseMessage protocol)
    {
        ResetTimer();

        return _map.TryGetValue(key, out protocol);
    }

    private void OnClearTimerElapsed(object sender, ElapsedEventArgs e)
    {
        ClearCache();
    }

    private void ClearCache()
    {
        _map.Clear();
    }

    private void ResetTimer()
    {
        clearTimer.Stop();
        clearTimer.Start();
    }
}