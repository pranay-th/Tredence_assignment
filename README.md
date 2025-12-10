# Workflow Engine — Data Quality Pipeline

This project implements a lightweight workflow engine as described in the assignment. The goal is to demonstrate clean backend design, clear execution flow, and simple state-driven logic without unnecessary complexity. Everything here focuses only on what the assignment asked for, but presented in a clear and well-documented way.

---

## Overview

The engine allows you to:

* Define nodes (each is a Python function that reads and updates shared state)
* Connect nodes using edges
* Add simple branching based on conditions
* Loop through nodes until a stopping condition is reached
* Run workflows through FastAPI endpoints

A small example workflow — **Data Quality Pipeline** — is included to show the engine working end-to-end.

The design focuses on readability, correctness, and predictable execution, not advanced features.

---

## Core Concepts

### **1. Nodes**

A node represents a step in the workflow. Each node points to a Python function (a tool) and receives:

* The current shared state
* Optional metadata

### **2. Shared State**

A dictionary passed from one node to the next. Each node may modify it.

### **3. Edges**

Define the execution order. An edge may include a condition such as:

```json
{"key": "anomaly_count", "op": ">", "value": 2}
```

If the condition is true, that edge is taken.

### **4. Looping**

Loops naturally occur by routing a node back to a previous node until a condition changes.

### **5. Tool Registry**

A simple dictionary storing the functions that nodes execute.

---

## API Endpoints

### **POST /graph/create**

Registers a workflow.

Body structure:

```json
{
  "nodes": { ... },
  "edges": { ... },
  "start_node": "profile"
}
```

Returns:

```json
{"graph_id": "..."}
```

### **POST /graph/run**

Starts executing a graph in the background.

```json
{
  "graph_id": "...",
  "initial_state": { ... }
}
```

Returns a `run_id`.

### **GET /graph/state/{run_id}**

Returns the current state, status, and logs of the run.

### **WebSocket: /ws/logs/{run_id}**

Streams execution logs step-by-step.

This is optional in the assignment, but included for clarity.

---

## Example Workflow: Data Quality Pipeline

This example shows how a real workflow looks in this engine. It includes:

1. Profiling data
2. Detecting anomalies
3. Generating rules
4. Applying rules
5. Looping until the number of anomalies falls below a threshold

### Running the demo

Start a run:

```bash
POST /demo/run_data_quality
```

You get:

```json
{"run_id": "..."}
```

Check progress:

```bash
GET /graph/state/{run_id}
```

Stream logs:

```bash
ws://localhost:8000/ws/logs/{run_id}
```

This provides a clear, observable execution trace from start to finish.

---

## How to Run Locally

1. Create a virtual environment:

```bash
python -m venv .venv
source .venv/bin/activate
```

2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Start the server:

```bash
uvicorn app.main:app --reload --port 8000
```

You are now ready to create and execute workflows.

---

## What This Engine Covers (as expected in the assignment)

* Clean separation of workflow logic, tools, engine, and API
* Nodes + edges + branching + looping
* Shared state mutation through the workflow
* Background execution
* Execution logs
* Simple and readable code structure

Everything included directly supports the assignment’s expectations without adding unrelated features.

---

## Final Notes

This project’s purpose is clarity and structure. The codebase is intentionally direct, well-documented, and easy to follow. It demonstrates workflow execution mechanics without hiding complexity behind abstractions.

If you want the README to sound more formal, more concise, or more casual, it can be rewritten accordingly.
