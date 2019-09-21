using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace Shielded.Gossip
{
    internal delegate ComplexRelationship ApplyDelegate(string key, object obj, bool deleted = false,
        bool expired = false, int? expiryMs = null);

    /// <summary>
    /// Helper for generic methods of signature:
    /// ComplexRelationship SomeMethod&lt;TItem&gt;(string key, FieldInfo&lt;TItem&gt; value).
    /// Produces a delegate which receives an object arg and separate info fields, and
    /// calls the appropriate version of the generic method.
    /// </summary>
    internal class ApplyMethods
    {
        public ApplyMethods(Expression<Func<string, FieldInfo<Lww<int>>, ComplexRelationship>> sampleCall)
        {
            var methodCall = (MethodCallExpression)sampleCall.Body;
            _self = methodCall.Object == null ? null :
                Expression.Lambda<Func<object>>(methodCall.Object).Compile()();
            _itemMsgMethod = methodCall.Method.GetGenericMethodDefinition();
        }

        public ApplyDelegate Get(Type t)
        {
            return _applyDelegates.GetOrAdd(t, CreateSetter);
        }

        private readonly ConcurrentDictionary<Type, ApplyDelegate> _applyDelegates = new ConcurrentDictionary<Type, ApplyDelegate>();
        private readonly object _self;
        private readonly MethodInfo _itemMsgMethod;

        private ApplyDelegate CreateSetter(Type t)
        {
            var methodInfo = _itemMsgMethod.MakeGenericMethod(t);
            var genericMethod = typeof(ApplyMethods).GetMethod("CreateSetterGeneric", BindingFlags.NonPublic | BindingFlags.Instance);
            MethodInfo genericHelper = genericMethod.MakeGenericMethod(t);
            return (ApplyDelegate)genericHelper.Invoke(this, new object[] { _self, methodInfo });
        }

        private ApplyDelegate CreateSetterGeneric<TItem>(object self, MethodInfo setter)
            where TItem : IMergeable<TItem>
        {
            var setterTypedDelegate = (Func<string, FieldInfo<TItem>, ComplexRelationship>)
                Delegate.CreateDelegate(typeof(Func<string, FieldInfo<TItem>, ComplexRelationship>), self, setter);
            ApplyDelegate res = (key, obj, del, exp, expMs) => setterTypedDelegate(key, new FieldInfo<TItem>((TItem)obj, del, exp, expMs));
            return res;
        }
    }
}
