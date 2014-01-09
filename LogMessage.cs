using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Inedo.Diagnostics;

namespace Inedo.BuildMasterExtensions.SqlServer
{
    [Serializable]
    internal sealed class LogMessage
    {
        public LogMessage(MessageLevel level, string message)
        {
            this.Level = level;
            this.Message = message;
        }

        public MessageLevel Level { get; private set; }
        public string Message { get; private set; }
    }
}
