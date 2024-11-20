package domain

// domain package contains the Domain Models and Interfaces for the Knitfab application.
//
// `domain/knitfab` package exposes root object for the Knitfab application.
// Entrypoints of applications should instantiage the Knitfab object and use it to interact with the domain.
//
// `domain/ENTITY.go` has high-level entities (Domain Model types) and functions.
// For example, `domain/data.go` contains the `Data` entity.
//
// `domain/ENTITY` directory contains the "phisical" representation of the domain entities, the RDB or Kubernetes(k8s).
// For example, `domain/data/db.go` contains the database expression of the data entity described in `domain/data.go`,
// and `domain/data/k8s.go` contains the Kubernetes expression of.
//
// `domain/ENTITY/interface.go` exposes the client interface to handle the domain entity in DB/k8s.
//
// # Entities
//
// Core entities in the domain are:
//
// - `data`: Any Data to be input to/output from ML tasks. They can be Tagged.
// in K8s, Data is represented by PVC.
// Knitfab watches Data and its Tags, and when Knitfab detects that the Data has all the Tags required by a Plan(ML Task),
// Knitfab "nominates" (= record assignabilities) the Data to the Plan Input (occur whenever Data is (re-)tagged and Data created on Run getting done).
// Then, Knitfab detects that the Plan is ready to run, starts it (occur in "loops").
//
// - `plan`: Definition of ML tasks.
// Standard Plans are a bundle of ML tasks in a container image and spesc of Input/Output Data.
// Inputs are tagged, and Data can be assignable when the Data has all the Tags set on the Plan. So, Tags of Input are "requirements" for Data from Plan Input.
// Ouutput are also tagged. When Run is done, the Data is created and tagged with the Output Tags.
//
// - `run`: Execution of ML tasks and the record of the execution.
// This includes the input/output Data and the Plan.
// When "projection loop" detect a new Input+Plan combinations which are different from any existing Runs, such Runs are created as new Runs.
// Once a Run is created, the "initialize loop" prepares output PVC for the Run, then "run management loop" will start the Run as Kubernetes Pod and watch it.
// When the Pod is done, the "finishing loop" will finalize the Run and set Tags on the output Data.
//
// And others:
//
// - `garbage`: Manages garbage collection of unused PVC which was Data.
// Once Run is removed, the PVCs for Output Data are not needed anymore. So, Knitfab will remove the PVC (occur in "gc loop").
//
// - `keychain`: Manages signkeys for JWT based on K8s secret. This is used to create JWT tokens for `/api/backend/data/import/*`.
//
// - `loop`: Manages recurring tasks. This defines constants for each loop.
// Implementation of the loop is in `cmd/loops/tasks/` directory.
//
// - `nomination`: Tracks which Data can be used by Plan(ML Task) Input.
//
// - `tags`: Representations of Tags for Data and Plan.
//
