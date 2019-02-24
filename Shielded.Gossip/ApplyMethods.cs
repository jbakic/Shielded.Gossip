﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Reflection;
using System.Text;

namespace Shielded.Gossip
{
    public delegate VectorRelationship ApplyDelegate(string key, object obj, bool delete = false, int? expiryMs = null);

    /// <summary>
    /// Helper for generic methods of signature:
    /// VectorRelationship SomeMethod&lt;TItem&gt;(string key, FieldInfo&lt;TItem&gt; value).
    /// Produces a delegate which receives an object arg and separate info fields, and
    /// calls the appropriate version of the generic method.
    /// </summary>
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
            where TItem : IMergeable<TItem>
        {
            var setterTypedDelegate = (Func<string, FieldInfo<TItem>, VectorRelationship>)
                Delegate.CreateDelegate(typeof(Func<string, FieldInfo<TItem>, VectorRelationship>), self, setter);
            ApplyDelegate res = (key, obj, del, exp) => setterTypedDelegate(key, new FieldInfo<TItem>((TItem)obj, del, exp));
            return res;
        }
    }
}
