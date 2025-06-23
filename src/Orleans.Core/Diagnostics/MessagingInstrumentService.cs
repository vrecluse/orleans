using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Messaging;
using Orleans.Serialization.Invocation;

namespace Orleans.Runtime;

public interface IMessagingInstrumentAttributeProvider
{
    string GetMessageType(object body);
}

public class MessagingInstrumentAttributeProvider : IMessagingInstrumentAttributeProvider
{
    public string GetMessageType(object body) => body.GetType().Name;
}

internal interface IMessagingInstrumentService
{
    void OnMessageReceive(Message msg, int numTotalBytes, int headerBytes, ConnectionDirection connectionDirection,
        SiloAddress remoteSiloAddress = null);

    void OnMessageSend(Message msg, int numTotalBytes, int headerBytes, ConnectionDirection connectionDirection,
        SiloAddress remoteSiloAddress = null);
}

internal class MessagingInstrumentService : IMessagingInstrumentService
{
    private IMessagingInstrumentAttributeProvider _attributeProvider;
    private readonly IOptionsMonitor<ClusterInstrumentOptions> _instrumentOptions;

    internal MessagingInstrumentService(IMessagingInstrumentAttributeProvider attributeProvider,
        IOptionsMonitor<ClusterInstrumentOptions> instrumentOptions)
    {
        _attributeProvider = attributeProvider;
        _instrumentOptions = instrumentOptions;
    }

    public void OnMessageReceive(Message msg, int numTotalBytes, int headerBytes,
        ConnectionDirection connectionDirection, SiloAddress remoteSiloAddress = null)
    {
        var detailed = _instrumentOptions.CurrentValue.DetailedMessageReceived;
        if (MessagingInstruments.MessageReceivedSizeHistogram.Enabled)
        {
            if (detailed && msg.InterfaceType is { })
            {
                var tagList = new System.Diagnostics.TagList();
                tagList.Add("ConnectionDirection", connectionDirection.ToString());
                tagList.Add("MessageDirection", msg.Direction.ToString());
                if (remoteSiloAddress != null)
                {
                    tagList.Add("silo", remoteSiloAddress);
                }

                var messageTag = GetMessageTag(msg);
                if (messageTag != null)
                {
                    tagList.Add("Message", messageTag);
                }

                MessagingInstruments.MessageReceivedSizeHistogram.Record(numTotalBytes, tagList);
            }
            else
            {
                if (remoteSiloAddress != null)
                {
                    MessagingInstruments.MessageReceivedSizeHistogram.Record(numTotalBytes,
                        new KeyValuePair<string, object>("ConnectionDirection", connectionDirection.ToString()),
                        new KeyValuePair<string, object>("MessageDirection", msg.Direction.ToString()),
                        new KeyValuePair<string, object>("silo", remoteSiloAddress));
                }
                else
                {
                    MessagingInstruments.MessageReceivedSizeHistogram.Record(numTotalBytes,
                        new KeyValuePair<string, object>("ConnectionDirection", connectionDirection.ToString()),
                        new KeyValuePair<string, object>("MessageDirection", msg.Direction.ToString()));
                }
            }
        }

        Interlocked.Add(ref MessagingInstruments._headerBytesReceived, headerBytes);
    }

    public void OnMessageSend(Message msg, int numTotalBytes, int headerBytes, ConnectionDirection connectionDirection,
        SiloAddress remoteSiloAddress = null)
    {
        Debug.Assert(numTotalBytes >= 0, $"OnMessageSend(numTotalBytes={numTotalBytes})");
        var detailed = _instrumentOptions.CurrentValue.DetailedMessageSent;

        if (MessagingInstruments.MessageSentSizeHistogram.Enabled)
        {
            if (detailed)
            {
                var tagList = new System.Diagnostics.TagList();
                tagList.Add("ConnectionDirection", connectionDirection.ToString());
                tagList.Add("MessageDirection", msg.Direction.ToString());
                if (remoteSiloAddress != null)
                {
                    tagList.Add("silo", remoteSiloAddress);
                }

                var messageTag = GetMessageTag(msg);
                if (messageTag != null)
                {
                    tagList.Add("Message", GetMessageTag(msg));
                }

                MessagingInstruments.MessageSentSizeHistogram.Record(numTotalBytes, tagList);
            }
            else
            {
                if (remoteSiloAddress != null)
                {
                    MessagingInstruments.MessageSentSizeHistogram.Record(numTotalBytes,
                        new KeyValuePair<string, object>("ConnectionDirection", connectionDirection.ToString()),
                        new KeyValuePair<string, object>("MessageDirection", msg.Direction.ToString()),
                        new KeyValuePair<string, object>("silo", remoteSiloAddress));
                }
                else
                {
                    MessagingInstruments.MessageSentSizeHistogram.Record(numTotalBytes,
                        new KeyValuePair<string, object>("ConnectionDirection", connectionDirection.ToString()),
                        new KeyValuePair<string, object>("MessageDirection", msg.Direction.ToString()));
                }
            }
        }

        Interlocked.Add(ref MessagingInstruments._headerBytesSent, headerBytes);
    }

    private string GetMessageTag(Message msg)
    {
        if (msg.BodyObject is IInvokable request)
        {
            return request.GetActivityName();
        }

        if (msg.BodyObject is Response response)
        {
            var result = response.Result;
            if (result == null)
            {
                result = response.Exception;
            }

            if (result == null)
            {
                result = response;
            }

            if (msg.InterfaceType.IsDefault)
            {
                return _attributeProvider.GetMessageType(result);
            }

            return $"{msg.InterfaceType.ToString()}:{_attributeProvider.GetMessageType(result)}";
        }

        if (msg.BodyObject != null)
        {
            return _attributeProvider.GetMessageType(msg.BodyObject);
        }

        return msg.InterfaceType.ToString();
    }
}
