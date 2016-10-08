The Spring Data DynamoDB JSON is a DynamoDB implementation of Spring repository interfaces.
It differentiates itself from (Spring Data DynamoDB)[https://github.com/derjust/spring-data-dynamodb] in the following ways:
- uses the Document SDK instead of the Mapper for ORM
- supports update expressions
- supports JSON patch operations by directly converting them to update expressions, making it easier to implement the HTTP PATCH verb in controllers
- fine grained exception handling
- only supports partition key schemas today
