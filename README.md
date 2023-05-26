- Does auto.register.schema has effect on consumer
- Does use.latest.version has effect on consumer

# Producer
- when auto.register.schema=true
  - of course, its own version becomes latest?
  - if its version has never been registered, then yes
  - if its version has been registered, let's say, latest-1, what happens?
- when auto.register.schema=false
  - when use.latest.version=false
  - when use.latest.version=true