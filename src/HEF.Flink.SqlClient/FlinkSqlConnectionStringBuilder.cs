using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;

namespace HEF.Flink.SqlClient
{
    public class FlinkSqlConnectionStringBuilder : DbConnectionStringBuilder
    {
		public FlinkSqlConnectionStringBuilder()
		{
		}

		public FlinkSqlConnectionStringBuilder(string connectionString)
		{
			ConnectionString = connectionString;
		}

		public string Server
        {
			get => FlinkSqlConnectionStringOption.Server.GetValue(this);
			set => FlinkSqlConnectionStringOption.Server.SetValue(this, value);
		}

		public int Port
        {
			get => FlinkSqlConnectionStringOption.Port.GetValue(this);
			set => FlinkSqlConnectionStringOption.Port.SetValue(this, value);
		}

		public string Planner
        {
			get => FlinkSqlConnectionStringOption.Planner.GetValue(this);
			set => FlinkSqlConnectionStringOption.Planner.SetValue(this, value);
		}

		public override bool ContainsKey(string key)
		{
			var option = FlinkSqlConnectionStringOption.TryGetOptionForKey(key);
			return option is object && base.ContainsKey(option.Key);
		}

		public override bool Remove(string key)
		{
			var option = FlinkSqlConnectionStringOption.TryGetOptionForKey(key);
			return option is object && base.Remove(option.Key);
		}

		public override object this[string key]
		{
			get => FlinkSqlConnectionStringOption.GetOptionForKey(key).GetObject(this);
			set
			{
				var option = FlinkSqlConnectionStringOption.GetOptionForKey(key);
				if (value is null)
					base[option.Key] = null;
				else
					option.SetObject(this, value);
			}
		}

		internal void DoSetValue(string key, object value) => base[key] = value;
	}

	internal abstract class FlinkSqlConnectionStringOption
    {
        #region Static
        private static readonly IDictionary<string, FlinkSqlConnectionStringOption> _optionDict;

		internal static readonly FlinkSqlConnectionStringOption<string> Server;
		internal static readonly FlinkSqlConnectionStringOption<int> Port;
		internal static readonly FlinkSqlConnectionStringOption<string> Planner;

		private static void AddOption(FlinkSqlConnectionStringOption option)
		{
			foreach (string key in option.Keys)
				_optionDict.Add(key, option);
		}

		static FlinkSqlConnectionStringOption()
        {
			_optionDict = new Dictionary<string, FlinkSqlConnectionStringOption>(StringComparer.OrdinalIgnoreCase);

			AddOption(Server = new FlinkSqlConnectionStringOption<string>(
				keys: new[] { "Server", "Host", "Data Source", "DataSource", "Address", "Addr", "Network Address" },
				defaultValue: ""));

			AddOption(Port = new FlinkSqlConnectionStringOption<int>(
				keys: new[] { "Port" },
				defaultValue: 8083));

			AddOption(Planner = new FlinkSqlConnectionStringOption<string>(
				keys: new[] { "Planner" },
				defaultValue: "blink"));
		}

		internal static FlinkSqlConnectionStringOption TryGetOptionForKey(string key) =>
			_optionDict.TryGetValue(key, out var option) ? option : null;

		internal static FlinkSqlConnectionStringOption GetOptionForKey(string key) =>
			TryGetOptionForKey(key) ?? throw new ArgumentException($"Option '{key}' not supported.");
        #endregion

        protected FlinkSqlConnectionStringOption(IReadOnlyList<string> keys)
		{
			Keys = keys;
		}

		internal string Key => Keys[0];

		internal IReadOnlyList<string> Keys { get; private set; }

		internal abstract object GetObject(FlinkSqlConnectionStringBuilder builder);
		internal abstract void SetObject(FlinkSqlConnectionStringBuilder builder, object value);
	}

	internal class FlinkSqlConnectionStringOption<T> : FlinkSqlConnectionStringOption
    {
		private readonly T _defaultValue;

		internal FlinkSqlConnectionStringOption(IReadOnlyList<string> keys, T defaultValue)
			: base(keys)
        {
			_defaultValue = defaultValue;
        }

		internal T GetValue(FlinkSqlConnectionStringBuilder builder) =>
			builder.TryGetValue(Key, out var objectValue) ? ChangeType(objectValue) : _defaultValue;

		internal void SetValue(FlinkSqlConnectionStringBuilder builder, T value) =>
			builder.DoSetValue(Key, value);

		internal override object GetObject(FlinkSqlConnectionStringBuilder builder) => GetValue(builder);

		internal override void SetObject(FlinkSqlConnectionStringBuilder builder, object value) => SetValue(builder, ChangeType(value));

		private static T ChangeType(object objectValue) =>
			(T)Convert.ChangeType(objectValue, typeof(T), CultureInfo.InvariantCulture);
	}
}
