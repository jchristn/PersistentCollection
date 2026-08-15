namespace Test.Shared
{
    using System;

    /// <summary>
    /// Simple serializable complex type used to exercise the collections with reference types.
    /// </summary>
    public class Person
    {
        /// <summary>
        /// Name.
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Age.
        /// </summary>
        public int Age { get; set; }

        /// <summary>
        /// Equality by value.
        /// </summary>
        /// <param name="obj">Other object.</param>
        /// <returns>True if equal.</returns>
        public override bool Equals(object obj)
        {
            if (obj is Person other) return Name == other.Name && Age == other.Age;
            return false;
        }

        /// <summary>
        /// Hash code.
        /// </summary>
        /// <returns>Hash code.</returns>
        public override int GetHashCode()
        {
            return (Name?.GetHashCode() ?? 0) ^ Age.GetHashCode();
        }

        /// <summary>
        /// String representation.
        /// </summary>
        /// <returns>String.</returns>
        public override string ToString()
        {
            return Name + " (" + Age + ")";
        }
    }
}
