using System;
using Skyline.DataMiner.Analytics.GenericInterface;
using Skyline.DataMiner.Net;
using Skyline.DataMiner.Net.Messages;
using System.Diagnostics;
using System.Text;
using System.Collections.Generic;


[GQIMetaData(Name = "Parameter KeyToName")]
public class ParameterKeyToName : IGQIRowOperator, IGQIInputArguments, IGQIColumnOperator, IGQIOnInit
{
    private readonly GQIColumnDropdownArgument _ParameterKeyColumnArg = new GQIColumnDropdownArgument("Parameter Key Column") { IsRequired = true };
    private readonly GQIColumnDropdownArgument _ProtocolColumnArg = new GQIColumnDropdownArgument("Protocol Name") { IsRequired = false };
    private readonly GQIColumnDropdownArgument _ProtocolVersionColumnArg = new GQIColumnDropdownArgument("Protocol Version") { IsRequired = false };
    private readonly GQIStringColumn _ParameterNameColumn = new GQIStringColumn("Parameter Name");

    private GQIColumn _parameterKeyColumn;
    private GQIColumn _protocolColumn;
    private GQIColumn _protocolVersionColumn;
    private Exception _LastException;

    private GQIDMS _dms;
    private IConnection _Conn;
    private ProtocolCache _protocolCache = new ProtocolCache();

    public GQIArgument[] GetInputArguments()
    {
        return new GQIArgument[] { _ParameterKeyColumnArg, _ProtocolColumnArg, _ProtocolVersionColumnArg };
    }

    public OnArgumentsProcessedOutputArgs OnArgumentsProcessed(OnArgumentsProcessedInputArgs args)
    {
        _parameterKeyColumn = args.GetArgumentValue(_ParameterKeyColumnArg);
        _protocolColumn = args.GetArgumentValue(_ProtocolColumnArg);
        _protocolVersionColumn = args.GetArgumentValue(_ProtocolVersionColumnArg);

        return new OnArgumentsProcessedOutputArgs();
    }

    public void HandleColumns(GQIEditableHeader header)
    {
        header.AddColumns(_ParameterNameColumn);
    }

    public OnInitOutputArgs OnInit(OnInitInputArgs args)
    {
        try
        {
            _dms = args.DMS;
            if (_dms != null)
            {
                //_Conn = _dms.GetConnection();
            }

        }
        catch (Exception ex)
        {
            _LastException = ex;
        }

        return new OnInitOutputArgs();
    }

    public void HandleRow(GQIEditableRow row)
    {
        try
        {
            //Init name with Key
            String parameterKey = row.GetValue<String>(_parameterKeyColumn);
            ParamID paramID = ParamID.FromString(parameterKey);

            // Find the correct protocol, based on the columns that have been linked this can be either a raw protocol (better performance/caching) or an element protocol
            GetProtocolInfoResponseMessage protocolInfo = null;
            if (_protocolColumn != null && _protocolVersionColumn != null)
            {//by protocol				
                String protocol = row.GetValue<String>(_protocolColumn);
                String version = row.GetValue<String>(_protocolVersionColumn);

                if (!_protocolCache.tryGet($"{protocol}/{version}", out protocolInfo))
                {
                    protocolInfo = _Conn.GetProtocol(protocol, version);
                    _protocolCache.Cache($"{protocol}/{version}", protocolInfo);
                }
            }
            else if (!_protocolCache.tryGet($"{paramID.DataMinerID}/{paramID.EID}", out protocolInfo))
            {//by element
                protocolInfo = _Conn.GetElementProtocol(paramID.DataMinerID, paramID.EID);
                _protocolCache.Cache($"{paramID.DataMinerID}/{paramID.EID}", protocolInfo);
            }

            //Once we have the protocol, lookup the map IDToName
            String parameterName = parameterKey;
            if (protocolInfo != null)
            {
                parameterName = protocolInfo.GetParameterName(paramID.PID);
            }

            row.SetValue<String>(_ParameterNameColumn, parameterName);

        }
        catch (Exception ex) { row.SetValue<String>(_ParameterNameColumn, "Some parameter name"); }
    }
}

public class ProtocolCache
{
    private Dictionary<String, GetProtocolInfoResponseMessage> _Map = new Dictionary<String, GetProtocolInfoResponseMessage>();

    public ProtocolCache()
    { }

    public void Cache(String key, GetProtocolInfoResponseMessage protocol)
    {
        lock (_Map)
        {
            _Map[key] = protocol;
        }
    }

    public bool tryGet(String key, out GetProtocolInfoResponseMessage protocol)
    {
        lock (_Map)
        {
            return _Map.TryGetValue(key, out protocol);
        }
    }
}