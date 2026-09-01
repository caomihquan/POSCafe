var builder = DistributedApplication.CreateBuilder(args);

var postgres = builder.AddPostgres("postgres");
var mongo = builder.AddMongoDB("mongo")
    .WithArgs("--replSet", "rs0", "--bind_ip_all")
    .WithInitFiles("mongo-init.js");
var kafka = builder.AddKafka("kafka");

var catalogDb = postgres.AddDatabase("catalogdb");
var identityDb = postgres.AddDatabase("identitydb");
var orderDb = postgres.AddDatabase("orderdb");
var opsDb = postgres.AddDatabase("opsdb");
var paymentDb = postgres.AddDatabase("paymentdb");
var storeDb = postgres.AddDatabase("storedb");
var inventoryDb = postgres.AddDatabase("inventorydb");
var catalogReadDb = mongo.AddDatabase("catalogread");

var catalog = builder.AddProject<Projects.PosCafe_Catalog_Api>(
    "catalog")
    .WithReference(catalogDb)
    .WithReference(catalogReadDb)
    .WaitFor(postgres)
    .WaitFor(mongo);

var order = builder.AddProject<Projects.PosCafe_Order_Api>(
    "order")
    .WithReference(orderDb)
    .WithReference(kafka)
    .WaitFor(postgres)
    .WaitFor(kafka);

var payment = builder.AddProject<Projects.PosCafe_Payment_Api>(
    "payment")
    .WithReference(paymentDb)
    .WithReference(kafka)
    .WaitFor(postgres)
    .WaitFor(kafka);

var identity = builder.AddProject<Projects.PosCafe_Identity_Api>("identity")
    .WithReference(identityDb)
    .WaitFor(postgres);
var store = builder.AddProject<Projects.PosCafe_Store_Api>("store")
    .WithReference(storeDb)
    .WaitFor(postgres);
var inventory = builder.AddProject<Projects.PosCafe_Inventory_Api>("inventory")
    .WithReference(inventoryDb)
    .WithReference(kafka)
    .WaitFor(postgres)
    .WaitFor(kafka);
var kitchen = builder.AddProject<Projects.PosCafe_Kitchen_Api>("kitchen");
var reporting = builder.AddProject<Projects.PosCafe_Reporting_Api>(
    "reporting")
    .WithReference(catalogReadDb)
    .WithReference(kafka)
    .WaitFor(mongo)
    .WaitFor(kafka);

builder.AddProject<Projects.PosCafe_ApiGateway>("gateway")
    .WithReference(opsDb)
    .WithReference(kafka)
    .WithReference(catalog)
    .WithReference(order)
    .WithReference(payment)
    .WithReference(identity)
    .WithReference(store)
    .WithReference(inventory)
    .WithReference(reporting)
    .WaitFor(catalog)
    .WaitFor(order)
    .WaitFor(payment)
    .WaitFor(identity)
    .WaitFor(store)
    .WaitFor(inventory)
    .WaitFor(reporting);

builder.Build().Run();
