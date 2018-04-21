using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Reflection;
using System.Text;

namespace Shielded.Gossip
{
    public delegate VectorRelationship ApplyDelegate(string key, object obj);

    public class ApplyMethods
    {
        public ApplyMethods(MethodInfo itemMsgMethod)
        {
            _itemMsgMethod = itemMsgMethod;
        }

        public ApplyDelegate Get(object self, Type t)
        {
            return _applyDelegates.GetOrAdd(t, _ => CreateSetter(self, t));
        }

        private readonly ConcurrentDictionary<Type, ApplyDelegate> _applyDelegates = new ConcurrentDictionary<Type, ApplyDelegate>();
        private readonly MethodInfo _itemMsgMethod;

        private ApplyDelegate CreateSetter(object self, Type t)
        {
            var methodInfo = _itemMsgMethod.MakeGenericMethod(t);
            var genericMethod = typeof(ApplyMethods).GetMethod("CreateSetterGeneric", BindingFlags.NonPublic | BindingFlags.Instance);
            MethodInfo genericHelper = genericMethod.MakeGenericMethod(t);
            return (ApplyDelegate)genericHelper.Invoke(this, new object[] { self, methodInfo });
        }

        private ApplyDelegate CreateSetterGeneric<TItem>(object self, MethodInfo setter)
            where TItem : IMergeable<TItem, TItem>
        {
            var setterTypedDelegate = (Func<string, TItem, VectorRelationship>)
                Delegate.CreateDelegate(typeof(Func<string, TItem, VectorRelationship>), self, setter);
            ApplyDelegate res = (key, obj) => setterTypedDelegate(key, (TItem)obj);
            return res;
        }
    }
}
