# Scala Crash Course for Spark Development

This document is intended to be a quick crash course for those intending to use the Scala programming language with Apache Spark.

## Table of Contents

- Overview
- Variables
- Classes
- Traits
- Functions/Closures
- Tuples
- Options
- Collections

### Overview

Scala is a language that blends object-oriented and functional programming.  It is statically typed, but makes heavy use of type inference to reduce code verbosity.  It is built on and interoperates with Java; that is, Scala compiles to Java bytecode, Scala requires a JVM to run, Scala code can use Java classes, and Java code can use Scala classes.

Scala supports not only class inheritance, but also traits, which are like partial classes, or interfaces with implementations, that contain both methods and fields that can be mixed in to other classes.  Think of traits as multiple inheritance with a better design.

Scala also places an emphasis on immutability in order to support parallelism, which is fundamental to writing systems that scale.  The need to execute tasks in parallel enables systems to process extremely large data sets.  If data is immutable, then there is no need to synchronize threads.

Scala also places very few restrictions on the characters allowed for method names.  Combined with dots for method invocations & parentheses around method arguments being optional, this allows Scala to support the notion of operator overloading offered in other languages and facilitates domain-specific languages (DSLs).

Further, it is worth noting that in Scala, assignments don't return the value assigned, they return `Unit`, which is a type that stands for "the absence of a type" (like `void` in Java).  Scala also provides a `Null` trait, with only one value, `null`, for backwards-compatibility with Java libraries.  Lastly, the root of Scala's inheritance hiearchy is `Any`, which has two subtypes, `AnyVal` (for primitives and `Unit`) and `AnyRef` (like `java.lang.Object`).

Scala also provides a command shell, or REPL (read-evaluate-print-loop), that we'll use in this introduction.  Simply type `scala` at a command prompt, and you should enter the interpretive shell that looks like this:

``` 
$ scala
Welcome to Scala version 2.11.7 (Java HotSpot(TM) 64-Bit Server VM, Java 1.8.0_31).
Type in expressions to have them evaluated.
Type :help for more information.

scala>
```

Now, let's get started.

### Variables

Variables can be defined as immutable or mutable, based on which keyword is used to declare it:  `val` or `var`.

#### Mutable variables

``` scala
var x = 0
x = 1 // ok: x is now 1
```

#### Immutable variables

``` scala
val y = 0
y = 1 // error: y is immutable!
```

### Classes

#### Regular classes

Define class:

``` 
class Greeter(private var myGreeting:String = "Hello", private var myTarget:String = "World") { // constructor with two optional arguments
  def greeting = myGreeting // getter
  def greeting_=(g: String) { myGreeting = g } // method name is literally "greeting ="
  def target = myTarget // getter
  def target_=(t: String) { myTarget = t } // method name is literally "target ="
  def greet = myGreeting + ", " + myTarget + "!" // one-liner method
  override def toString = greet // override base class's toString method
}
```

Use class:

``` scala
val g = new Greeter
g.greet
g.greeting =("Hi")
g greeting =("Hi") // same as above
g greeting = "Hi"  // same as above
g.greeting = "Hi"  // same as above
g.greet
```

#### Companion classes

Scala has no notion of class fields or class methods, which are fields or methods, respectively, that don't pertain to a given instance of the class but to all instances of the class (like `static` in Java).  Instead, Scala provides for the definition of a companion class, which is a singleton.  It has no constructors and can have fields & methods.  Any method called `apply` will be called when no method name is given after the companion class name in usage.

Define class & companion class (note the `:paste` is required by the Scala shell, because companion classes must be defined with their conventional class):

``` scala
:paste
class Greeter(private var myGreeting:String = "Hello", private var myTarget:String = "World") { // constructor with two optional arguments
  def greeting = myGreeting // getter
  def greeting_=(g: String) { myGreeting = g } // method name is literally "greeting ="
  def target = myTarget // getter
  def target_=(t: String) { myTarget = t } // method name is literally "target ="
  def greet = myGreeting + ", " + myTarget + "!" // one-liner method
  override def toString = greet // override base class's toString method
}
object Greeter {
  def apply(greeting: String = "Howdy", target: String = "Y'all")
    = new Greeter(greeting, target)
}
// now type Ctrl-D to finish declaration of class & companion
```

Use companion class:

``` scala
val g = Greeter() // no "new" required; "apply" method is called on companion
```

#### Case classes

Scala provides convenient, immutable classes with getters, equals, hash code & stringify methods called "case classes".

Define case class:

``` scala
case class Greeter(val greeting: String = "Hello", val target: String = "World") {
  def greet = greeting + ", " + target + "!"
}
```

Use case class:

``` scala
val g = Greeter()
g.greeting // getter
g.target // getter
g.toString // "Greeter(Hello,World)"
g.greet // "Hello, World!"
val h = Greeter("Howdy", "Y'all")
h.toString // "Greeter(Howdy,Y'all)"
h.greet // "Howdy, Y'all!"
val i = Greeter("Hello","World")
g == h // false
g == i // true
```

### Traits

Traits are Scala's mechanism of better multiple inheritance.  A `trait` is essentially an abstract partial class, containing zero more instance fields & methods that can be "mixed in" to another class.  Further, `traits` replace Java's notion of `interface`; there is no `interface` keyword in Scala.

A `class` can `extend` only zero or one other class, but a `class` can "express" as many `trait`s as necessary.  A `class` "expresses" `trait`s via the syntax  `class B extends A with T …`, where `A` is either a non-`final` `class` or a `trait`, and there can be zero or more `with` clauses, provided that the type after the keyword `with` is a `trait`.

Define an asbstract class & some traits:

``` scala
abstract class Pet(val name:String)
trait CanBark { def bark { println("Bark!") } }
trait CanMeow { def meow { println("Meow!") } }
trait CanFly { def fly(altitude:Int) { println(s"I'm flying at $altitude m!")}}
```

Define classes that express the trait(s):

``` scala
class BarkingCat(name:String) extends Pet(name) with CanMeow with CanBark
class MeowingDog(name:String) extends Pet(name) with CanBark with CanMeow
class WildFlyingMonkey(val name:String, val defaultAltitude:Int = 1000) extends CanFly {
  def fly() { fly(defaultAltitude) }
}
```

Use the classes:

``` scala
val cat = new BarkingCat("Butch")
cat.meow // Meow!
cat.bark // Bark!
cat.isInstanceOf[Pet] // true
cat.isInstanceOf[BarkingCat] // true
cat.isInstanceOf[CanMeow] // true
cat.isInstanceOf[CanBark] // true
cat.isInstanceOf[CanFly] // false
val dog = new MeowingDog("Fifi")
dog.bark // Bark!
dog.meow // Meow!
val monkey = new WildFlyingMonkey("Albert", 10000)
monkey.fly // I'm flying at 10000 m!
monkey.fly(10) // I'm flying at 10 m!
val flyingCat = new BarkingCat("Crazy") with CanFly // define class with trait at run-time!
flyingCat.fly(100) // I'm flying at 100 m!
flyingCat.isInstanceOf[CanFly] // true
```



### Functions/Closures

Scala supports values that are functions.  Another term for a function that uses values defined outside of the scope of the function is "closure", because it effectively closes over the variables in scope at the point of the function's definition.  The two are essentially synonymous.

Define a function:

``` scala
val factor = 2
def fun(x:Int) = factor * x // closes over in-scope variable "factor"
```

Use function:

``` scala
fun(0) // 0
fun(1) // 2
fun(2) // 4
```

#### Anonymous functions

Functions don't have to have names & they can be passed around like data.  They're defined with the `=>` syntax, which takes an argument list on the left, the `=>` symbol, then the body.

Define function that takes an anonymous function & executes it:

``` scala
def doSomething(fn:(Int) => Int, y:Int) = fn(y)
```

The type of `fn` can be read "a function that takes an `Int` and returns an `Int`".

Provide an anonymous function to `doSomething`:

``` scala
doSomething((x:Int) => 2 * x, 3) // 6
```

The anonymous function is `(x:Int) => 2 * x`, and we're passing `3` for `y`.  Try another:

``` scala
doSomething((x:Int) => -x, 1) // -1
```

### Tuples

Scala provides tuples, which are singles, pairs, triplets, quadruplets, quintuplets, etc. of data (up to 22 members) which are agnostic toward the data's type, and they're denoted by parentheses.  They have member access methods `_1()`, `_2()`, etc., up to `_22` — notice that they're one-based.

Define a tuple with three members:

``` scala
val triplet = ("one", 2.0, true)
```

The type of `triplet` is expressed as `(String, Double, Boolean)`.  Now, access `triplet`'s members:

``` scala
triplet._1 // "one"
triplet._2 // 2.0
triplet._3 // true
```

### Options

Scala only offers the `Null` trait for use with Java libraries that make use of Java's `null`.  In the absence of nullability, Scala provides `Option`.  It has two case subclasses, `Some` to represent the presence of a value, and `None` to represent the absence of a value.

Declare two optional values:

``` scala
val s: Option[Int] = Some(1)
val n: Option[Int] = None
```

Now, inspect them:

``` scala
s.isDefined // true
s.isEmpty // false
s.get // 1
n.isDefined // false
n.isEmpty // true
n.get // throws java.util.NoSuchElementException
n.getOrElse(-1) // -1 (good way to provide default value)
```

### Collections

Scala offers a rich collection library with many interesting methods in mutable and immutable versions; unless you specify with `import scala.collections.mutable`, you get the immutable versions.

There are three basic types, all of which descend from `Iterable`, which descends from  `Traversable`:

- `Seq`, an ordered collection
- `Set`, an unordered collection which contains no duplicates
- `Map`, key-value pairs, or a collection of values each of which is accessible via a key

Here's a quick overview of the Scala immutable collections (from http://docs.scala-lang.org/overviews/collections/overview.html):

![Immutable Collections](collections.immutable.png)

#### Processing collections

All of the methods you'd expect on collections are present.  Some of the more interesting methods are illustrated below.

##### Map:  transform elements

``` scala
val values = List(1, 2, 3, 4, 5, 6)
val plusones = values.map(x => x + 1) // List(2, 3, 4, 5, 6, 7)
val plusones2 = values.map(_ + 1) // same, except uses _ placeholder
val strings = values.map(_.toString) // List("1", "2", "3", "4", "5", "6")
```

##### Reduce:  combine elements

``` scala
val values = List(1, 2, 3, 4, 5, 6)
val sum = values.reduce((a, b) => a + b) // 21
val sum2 = values.reduce(_ + _) // 21 using shorthand
val factorial = values.reduce(_ * _) // 720
```

##### Sort:  order elements

``` scala
val values = List(1, 2, 3, 4, 5, 6)
val reversed = values.sortBy(x => -x) // List(6, 5, 4, 3, 2, 1)
val reversed2 = values.sortBy(-_) // same, except uses placeholder
  
val tuples = List((1, "foo"), (2, "bar"), (3, "sna"), (4, "fu"), (5, "beetlejuice"))

val sortedByReverseKey = tuples.sortBy(t => -t._1)
  // List((5,beetlejuice), (4,fu), (3,sna), (2,bar), (1,foo))
val sortedByReverseKey2 = tuples.sortBy(-_._1) // same, shorthand

val sortedByValue = tuples.sortBy(t => t._2)
  // List((2,bar), (5,beetlejuice), (1,foo), (4,fu), (3,sna))
val sortedByValue2 = tuples.sortBy(_._2) // same, shorthand
```

##### Filter:  select elements

``` scala
val values = List(1, 2, 3, 4, 5, 6)
val evens = values.filter(_ % 2 == 0) // List(2, 4, 6)
val odds = values.filter(_ % 2 != 0) // List (1, 3, 5)
```

