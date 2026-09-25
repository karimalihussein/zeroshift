(() => {
  const nav = document.getElementById("docs-nav");
  const main = document.getElementById("docs-content");
  const status = document.getElementById("docs-status");
  const toc = document.getElementById("docs-toc");
  const tocNav = document.getElementById("docs-toc-nav");
  const query = document.getElementById("docs-query");
  const results = document.getElementById("docs-results");
  const version = document.getElementById("docs-version");
  let catalog = null;
  let activeHit = 0;

  const theme = localStorage.getItem("zeroshift-docs-theme");
  if (theme === "dark") setTheme(true);

  document.getElementById("theme").addEventListener("click", () => {
    setTheme(document.documentElement.dataset.theme !== "dark");
  });
  document.querySelector(".nav-toggle").addEventListener("click", () => {
    const open = document.body.classList.toggle("nav-open");
    document.querySelector(".nav-toggle").setAttribute("aria-expanded", String(open));
  });
  document.addEventListener("click", (event) => {
    const link = event.target.closest("a");
    if (!link) return;
    const url = new URL(link.href, location.origin);
    if (url.origin === location.origin && url.pathname.startsWith("/docs")) {
      event.preventDefault();
      document.body.classList.remove("nav-open");
      go(url.pathname + url.hash);
    }
  });
  window.addEventListener("popstate", render);
  query.addEventListener("input", () => showSearch(query.value));
  query.addEventListener("keydown", (event) => {
    const links = [...results.querySelectorAll("a")];
    if (event.key === "ArrowDown") { event.preventDefault(); activeHit = Math.min(links.length - 1, activeHit + 1); mark(links); }
    if (event.key === "ArrowUp") { event.preventDefault(); activeHit = Math.max(0, activeHit - 1); mark(links); }
    if (event.key === "Enter" && links[activeHit]) { event.preventDefault(); links[activeHit].click(); hideSearch(); }
    if (event.key === "Escape") hideSearch();
  });
  document.addEventListener("keydown", (event) => {
    if (event.key === "/" && document.activeElement !== query && !event.metaKey && !event.ctrlKey) {
      event.preventDefault();
      query.focus();
    }
  });
  document.addEventListener("click", (event) => {
    if (!event.target.closest(".docs-search")) hideSearch();
  });

  fetch("/api/docs/catalog")
    .then((response) => {
      if (!response.ok) throw new Error("Catalog responded " + response.status);
      return response.json();
    })
    .then((body) => {
      catalog = body;
      version.textContent = body.version;
      drawNav();
      if (location.pathname === "/docs" || location.pathname === "/docs/") history.replaceState(null, "", "/docs/overview");
      render();
    })
    .catch((error) => {
      status.hidden = false;
      status.textContent = "The catalog did not load. " + error.message;
    });

  function setTheme(dark) {
    document.documentElement.dataset.theme = dark ? "dark" : "light";
    localStorage.setItem("zeroshift-docs-theme", dark ? "dark" : "light");
    const button = document.getElementById("theme");
    button.textContent = dark ? "Light" : "Dark";
    button.setAttribute("aria-pressed", String(dark));
  }

  function go(path) {
    history.pushState(null, "", path);
    render();
  }

  function drawNav() {
    nav.replaceChildren();
    for (const group of catalog.navigation) {
      const details = el("details", "nav-group");
      details.open = group.items.some((item) => item.href.split("#")[0] === location.pathname);
      const summary = el("summary");
      summary.textContent = group.label;
      details.append(summary);
      for (const item of group.items) {
        const link = el("a");
        link.href = item.href;
        const label = document.createElement("span");
        label.textContent = item.label;
        link.append(label);
        if (item.method) link.append(badge(item.method));
        details.append(link);
      }
      nav.append(details);
    }
  }

  function render() {
    if (!catalog) return;
    status.hidden = true;
    main.hidden = false;
    main.replaceChildren();
    const path = location.pathname.replace(/\/$/, "") || "/docs/overview";
    const parts = path.split("/").filter(Boolean);
    markCurrent(path);
    if (parts[1] === "api" && parts[2]) return operation(parts[2]);
    if (parts[1] === "services" && !parts[2]) return serviceCatalog();
    if (parts[1] === "services" && parts[2]) return service(parts[2]);
    if (parts[1] === "events" && !parts[2]) return eventIndex();
    if (parts[1] === "events" && parts[2]) return event(parts[2]);
    if (parts[1] === "kafka" && (!parts[2] || parts[2] === "topics" || parts[2] === "groups" || parts[2] === "outbox" || parts[2] === "dead-letters"))
      return kafka(parts[2] || "overview");
    const page = catalog.pages.find((item) => item.slug === parts[1]);
    if (!page) return missing();
    crumbs(page.group, page.title);
    heading(page.title, page.summary);
    markdown(main, page.markdown);
    if (page.slug === "errors") errorTable();
    if (page.slug === "architecture") serviceLinks();
    outline();
  }

  function operation(id) {
    const op = catalog.operations.find((item) => item.id === id);
    if (!op) return missing();
    document.title = op.title + " · ZeroShift docs";
    crumbs(op.group, op.title);
    const h = heading(op.title, op.summary);
    const line = el("p", "endpoint");
    line.append(badge(op.method), code(op.path));
    h.after(line);
    section("Description", (node) => node.append(text(op.summary)));
    section("Authentication", (node) => node.append(text(op.auth)));
    if (op.headers.length) section("Headers", (node) => node.append(paramTable(op.headers, true)));
    if (op.pathParams.length) section("Path parameters", (node) => node.append(paramTable(op.pathParams)));
    if (op.query.length) section("Query parameters", (node) => node.append(paramTable(op.query)));
    if (op.request) section("Request body", (node) => schemaBlock(node, op.request));
    section("Response", (node) => {
      if (!op.responses.length) node.append(text("This call does not document a response body."));
      for (const response of op.responses) {
        const title = el("h3");
        title.textContent = String(response.status);
        title.id = "status-" + response.status;
        node.append(title, text(response.description || ""));
        if (response.schema) schemaBlock(node, response.schema);
      }
    });
    if (op.errors.length) {
      section("Errors", (node) => {
        const table = document.createElement("table");
        table.append(row(["Status", "Code", "When"], true));
        for (const error of op.errors) {
          const tr = document.createElement("tr");
          tr.append(cell(String(error.status)), cellLink(error.code, "/docs/errors#" + error.code.toLowerCase()), cell(error.when));
          table.append(tr);
        }
        node.append(table);
      });
    }
    section("Idempotency", (node) => node.append(text(op.idempotency)));
    section("Consistency", (node) => node.append(text(op.consistency)));
    if (op.events.length) {
      section("Related events", (node) => {
        const list = document.createElement("ul");
        for (const type of op.events) {
          const item = document.createElement("li");
          const link = document.createElement("a");
          link.href = "/docs/events/" + slug(type);
          link.textContent = type;
          item.append(link);
          list.append(item);
        }
        node.append(list);
      });
    }
    section("Service", (node) => {
      const link = document.createElement("a");
      const service = op.service === "platform" ? "order-service" : op.service;
      link.href = "/docs/services/" + service;
      link.textContent = op.service;
      const source = el("p", "note");
      source.append(link, document.createTextNode(op.source ? " · " + op.source : ""));
      node.append(source);
    });
    section("Examples", (node) => samples(node, op));
    tryIt(op);
    outline();
  }

  function serviceCatalog() {
    document.title = "Services · ZeroShift docs";
    crumbs("Services", "Catalog");
    heading("Service catalog", "Each box is a process you can run. The arrows are the paths a placed order actually takes.");
    const flow = el("div", "flow");
    flow.append(flowRow(["Client", "POST /orders", "order-service", "PostgreSQL orders + outbox"]));
    flow.append(flowRow(["outbox", "Debezium", "Kafka", "payment-service", "inventory-service", "shipping-service"]));
    flow.append(flowRow(["payment.events", "order-saga", "inventory.events", "shipping.events"]));
    flow.append(flowRow(["order.events", "order-query-service"]));
    main.append(flow);
    const table = document.createElement("table");
    table.className = "catalog";
    table.append(row(["Service", "Port", "Database", "Produces", "Consumes"], true));
    for (const service of catalog.services) {
      const tr = document.createElement("tr");
      tr.append(cellLink(service.name, "/docs/services/" + service.id));
      tr.append(cell(String(service.port) + (service.replica ? " · " + service.replica : "")));
      tr.append(cell(service.database));
      tr.append(cell(service.produces.join(", ") || "—"));
      tr.append(cell(service.consumes.join(", ") || "—"));
      table.append(tr);
    }
    main.append(table);
    outline();
  }

  function service(id) {
    const item = catalog.services.find((service) => service.id === id);
    if (!item) return missing();
    document.title = item.name + " · ZeroShift docs";
    crumbs("Services", item.name);
    heading(item.name, item.purpose);
    const table = document.createElement("table");
    addFact(table, "Port", String(item.port) + (item.replica ? " (" + item.replica + ")" : ""));
    addFact(table, "Database", item.database);
    addFact(table, "Health", item.health);
    addFact(table, "Module", item.module);
    addFact(table, "HTTP", item.rest.join(", "));
    addFact(table, "Produces", item.produces.join(", ") || "Nothing. This service does not publish.");
    addFact(table, "Consumes", item.consumes.join(", ") || "No consumer group of its own.");
    addFact(table, "Depends on", item.dependsOn.join(", "));
    addFact(table, "Observability", item.observability);
    main.append(table);
    const related = catalog.operations.filter((op) => op.api && (op.service === id || (op.module === "platform" && id !== "migration-lab")));
    if (related.length) {
      section("REST", (node) => {
        const list = document.createElement("ul");
        for (const op of related) {
          const li = document.createElement("li");
          li.append(badge(op.method), document.createTextNode(" "));
          const link = document.createElement("a");
          link.href = "/docs/api/" + op.id;
          link.textContent = op.path + " — " + op.title;
          li.append(link);
          list.append(li);
        }
        node.append(list);
      });
    }
    outline();
  }

  function eventIndex() {
    document.title = "Events · ZeroShift docs";
    crumbs("Events", "Envelopes");
    heading("Events", "Every message type in Contracts. The wire document is an Envelope around the payload record.");
    markdown(main, "The envelope fields are `eventId`, `type`, `schemaVersion`, `correlationId`, `causationId`, `occurredAt` and `payload`. Kafka headers added by the outbox router are listed on each event. Version is the number of upcasters plus one.");
    const table = document.createElement("table");
    table.append(row(["Event", "Version", "Topic", "Producers", "Consumers"], true));
    for (const event of catalog.events) {
      const tr = document.createElement("tr");
      tr.append(cellLink(event.type, "/docs/events/" + slug(event.type)));
      tr.append(cell("v" + event.version));
      tr.append(cellLink(event.topic, "/docs/kafka/topics#" + slug(event.topic)));
      tr.append(cell(event.producers.join(", ")));
      tr.append(cell(event.consumers.join(", ")));
      table.append(tr);
    }
    main.append(table);
    outline();
  }

  function event(name) {
    const item = catalog.events.find((event) => slug(event.type) === name);
    if (!item) return missing();
    document.title = item.type + " · ZeroShift docs";
    crumbs("Events", item.type);
    heading(item.type, item.family + " · " + item.topic + " · schema v" + item.version);
    const table = document.createElement("table");
    addFact(table, "Topic", item.topic);
    addFact(table, "Partition key", item.partitionKey);
    addFact(table, "Producers", item.producers.join(", "));
    addFact(table, "Consumers", item.consumers.join(", "));
    addFact(table, "Dead letter", item.dlq);
    addFact(table, "Java type", item.javaType);
    main.append(table);
    section("Payload", (node) => {
      node.append(fieldTable(item.payload));
      labeledCode(node, "Example payload", item.example, "json");
    });
    section("Envelope", (node) => labeledCode(node, "Example envelope", item.envelope, "json"));
    section("Headers", (node) => {
      const list = document.createElement("ul");
      for (const header of item.headers) {
        const li = document.createElement("li");
        li.append(code(header));
        list.append(li);
      }
      node.append(list, text("These names are the header side of the outbox router's additional placement. The delivery-attempt header is added by the listener container, not by the router."));
    });
    section("Ordering", (node) => node.append(text(item.ordering)));
    section("Idempotency", (node) => node.append(text(item.idempotency)));
    section("Retries", (node) => node.append(text(item.retry)));
    if (item.upcasters.length) {
      section("Evolution", (node) => {
        const flow = el("div", "flow");
        flow.append(flowRow([item.type + " v1", "Upcaster", item.type + " v" + item.version]));
        node.append(flow);
        for (const line of item.upcasters) node.append(text(line));
      });
    }
    outline();
  }

  function kafka(which) {
    const titles = {
      overview: ["Kafka", "One broker for the commerce lab, and a separate 3-node cluster for the Kafka lab."],
      topics: ["Topics", "Partition counts and retention are read from infra/kafka/topics.sh and from LabTopics."],
      groups: ["Consumer groups", "A listener id is the group id. The control plane also tails the commerce topics."],
      outbox: ["Outbox", "The row is committed with the state change. Debezium publishes it."],
      "dead-letters": ["Dead letters", "Four retries, then {topic}.dlt. Malformed messages skip the retries."]
    };
    const [title, summary] = titles[which];
    document.title = title + " · ZeroShift docs";
    crumbs("Kafka", title);
    heading(title, summary);
    if (which === "overview") {
      const flow = el("div", "flow");
      flow.append(flowRow(["Producer", "Topic", "Partition 0"]));
      flow.append(flowRow(["Partition 1", "Partition 2", "Consumer group"]));
      main.append(flow, text("Commerce topics are created with replication factor 1, because that cluster has one broker. The kafka-lab topics are created with replication factor 2 or 3. A dead-letter topic has one partition."));
    }
    if (which === "overview" || which === "topics") topicTable();
    if (which === "groups") groupTable();
    if (which === "outbox") {
      markdown(main, "Connectors are `{database}-outbox` for the databases named in `infra/debezium/register.sh`. The connector reads `public.outbox`, publication `outbox_inserts`, slot `{database}_outbox`, and routes with `EventRouter`. `order-query-service` does not create a slot.\n\nThe message key is the outbox `aggregate_id`, which is the order id. Carrier scans are the exception: the shipping service publishes them with `KafkaTemplate`, not the outbox.");
    }
    if (which === "dead-letters") {
      const dead = catalog.topics.filter((topic) => topic.name.endsWith(".dlt"));
      const table = document.createElement("table");
      table.append(row(["Topic", "Partitions", "Retention", "Key"], true));
      for (const topic of dead) {
        const tr = document.createElement("tr");
        tr.id = slug(topic.name);
        tr.append(cell(topic.name), cell(String(topic.partitions)), cell(topic.retention), cell(topic.messageKey));
        table.append(tr);
      }
      main.append(table);
    }
    outline();
    if (location.hash) {
      const target = document.getElementById(location.hash.slice(1));
      if (target) target.scrollIntoView();
    }
  }

  function topicTable() {
    const table = document.createElement("table");
    table.append(row(["Topic", "Partitions", "Replication", "Retention", "Producers", "Groups", "Key", "DLQ"], true));
    for (const topic of catalog.topics) {
      const tr = document.createElement("tr");
      tr.id = slug(topic.name);
      tr.append(cell(topic.name));
      tr.append(cell(topic.partitions == null ? "—" : String(topic.partitions)));
      tr.append(cell(topic.replicationFactor || "—"));
      tr.append(cell(topic.retention));
      tr.append(cell(topic.producers.join(", ") || "—"));
      tr.append(cell(topic.consumerGroups.join(", ") || "—"));
      tr.append(cell(topic.messageKey));
      tr.append(cell(topic.deadLetter || "—"));
      table.append(tr);
    }
    main.append(table);
  }

  function groupTable() {
    const groups = new Map();
    for (const topic of catalog.topics) {
      for (const group of topic.consumerGroups) {
        if (!groups.has(group)) groups.set(group, []);
        groups.get(group).push(topic.name);
      }
    }
    const table = document.createElement("table");
    table.append(row(["Group", "Topics"], true));
    for (const [group, topics] of groups) {
      const tr = document.createElement("tr");
      tr.append(cell(group), cell([...new Set(topics)].join(", ")));
      table.append(tr);
    }
    main.append(table, text("order-saga listens to payment.events, inventory.events and shipping.events with concurrency order.saga-consumers (default 1). payment-service, inventory-service, shipping-service and order-projection use concurrency 3. carrier-tracking is a container the shipping service registers itself."));
  }

  function errorTable() {
    const table = document.createElement("table");
    table.append(row(["Code", "Status", "Scope", "When"], true));
    for (const error of catalog.errors) {
      const tr = document.createElement("tr");
      tr.id = error.code.toLowerCase();
      tr.append(cell(error.code), cell(String(error.status)), cell(error.scope), cell(error.when));
      table.append(tr);
    }
    main.append(el("h2", null, "Codes"), table);
  }

  function serviceLinks() {}

  function tryIt(op) {
    const node = el("section", "try");
    const title = el("h2");
    title.id = "try-it";
    title.textContent = "Try it";
    node.append(title);
    if (!op.tryIt) {
      node.append(text("This call changes a process, a cluster or a database in a way the portal will not send. Use the lab page that owns it."));
      main.append(node);
      return;
    }
    const form = document.createElement("form");
    const fields = el("div", "fields");
    if (op.hosts.length > 1) fields.append(labeled("Service", select(op.hosts), "host"));
    for (const param of op.pathParams) fields.append(labeled(param.name, input(param.name, ""), param.name));
    for (const param of op.query) fields.append(labeled(param.name + (param.defaultValue ? " (default " + param.defaultValue + ")" : ""), input(param.name, ""), "q-" + param.name));
    const headerBox = document.createElement("textarea");
    headerBox.rows = 3;
    headerBox.value = op.headers.some((header) => header.name === "Idempotency-Key") ? "Idempotency-Key: docs-" + Date.now() : "";
    fields.append(labeled("Headers", headerBox, "headers"));
    let bodyBox = null;
    if (op.request) {
      bodyBox = document.createElement("textarea");
      bodyBox.rows = 8;
      bodyBox.value = (op.request.example || "").trim();
      fields.append(labeled("JSON body", bodyBox, "body"));
    }
    const actions = el("div", "try-actions");
    const submit = document.createElement("button");
    submit.type = "submit";
    submit.textContent = "Send request";
    actions.append(submit);
    const meta = el("div", "try-meta");
    const result = el("div", "try-result");
    form.append(fields, actions, meta, result);
    form.addEventListener("submit", async (event) => {
      event.preventDefault();
      submit.disabled = true;
      meta.textContent = "Sending…";
      result.replaceChildren();
      const pathParams = {};
      for (const param of op.pathParams) pathParams[param.name] = form.elements[param.name].value;
      const query = {};
      for (const param of op.query) {
        const value = form.elements["q-" + param.name].value;
        if (value) query[param.name] = value;
      }
      const headers = {};
      for (const line of headerBox.value.split("\n")) {
        const index = line.indexOf(":");
        if (index > 0) headers[line.slice(0, index).trim()] = line.slice(index + 1).trim();
      }
      try {
        const response = await fetch("/api/docs/try", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            operation: op.id,
            host: form.elements.host ? form.elements.host.value : null,
            pathParams,
            query,
            headers,
            body: bodyBox ? bodyBox.value : null
          })
        });
        const payload = await response.json();
        meta.replaceChildren();
        const code = el("span", "status-code " + (payload.status >= 200 && payload.status < 300 ? "ok" : "err"));
        code.textContent = payload.status ? String(payload.status) : "unreachable";
        meta.append(code, document.createTextNode(" · " + payload.durationMs + " ms"));
        if (payload.requestId) meta.append(document.createTextNode(" · " + payload.requestId));
        if (payload.error) result.append(text(payload.error));
        if (payload.body) labeledCode(result, "Response", pretty(payload.body), "json");
        if (payload.headers && Object.keys(payload.headers).length) {
          const table = document.createElement("table");
          table.append(row(["Header", "Value"], true));
          for (const [name, value] of Object.entries(payload.headers)) {
            const tr = document.createElement("tr");
            tr.append(cell(name), cell(value));
            table.append(tr);
          }
          result.append(el("h3", null, "Response headers"), table);
        }
      } catch (error) {
        meta.textContent = error.message;
      } finally {
        submit.disabled = false;
      }
    });
    node.append(form);
    main.append(node);
  }

  function samples(node, op) {
    const host = op.hosts.length ? "http://localhost:" + port(op.hosts[0]) : "http://localhost:8080";
    const curl = curlOf(op, host);
    const java = javaOf(op, host);
    const js = jsOf(op, host);
    const tabs = el("div", "tabs");
    const view = el("div");
    const languages = [["curl", curl, "bash"], ["Java", java, "java"], ["JavaScript", js, "js"]];
    languages.forEach(([label, source, lang], index) => {
      const button = document.createElement("button");
      button.type = "button";
      button.textContent = label;
      button.setAttribute("aria-selected", index === 0 ? "true" : "false");
      button.addEventListener("click", () => {
        tabs.querySelectorAll("button").forEach((item) => item.setAttribute("aria-selected", "false"));
        button.setAttribute("aria-selected", "true");
        view.replaceChildren();
        labeledCode(view, label, source, lang);
      });
      tabs.append(button);
    });
    node.append(tabs, view);
    labeledCode(view, "curl", curl, "bash");
  }

  function curlOf(op, host) {
    const lines = ["curl -sS -D - " + JSON.stringify(host + op.path)];
    if (op.request) lines.push("  -H 'content-type: application/json'");
    if (op.headers.some((header) => header.name === "Idempotency-Key")) lines.push("  -H 'Idempotency-Key: docs-demo-1'");
    if (op.request && op.request.example) lines.push("  -d " + JSON.stringify(op.request.example.trim()));
    return lines.join(" \\\n");
  }

  function javaOf(op, host) {
    return [
      "var client = HttpClient.newHttpClient();",
      "var request = HttpRequest.newBuilder(URI.create(\"" + host + op.path + "\"))",
      op.request ? "    .header(\"content-type\", \"application/json\")" : null,
      "    .method(\"" + op.method + "\", " + (op.request ? "HttpRequest.BodyPublishers.ofString(body)" : "HttpRequest.BodyPublishers.noBody()") + ")",
      "    .build();",
      "var response = client.send(request, HttpResponse.BodyHandlers.ofString());"
    ].filter(Boolean).join("\n");
  }

  function jsOf(op, host) {
    const init = ["method: \"" + op.method + "\""];
    if (op.request) init.push("headers: { \"content-type\": \"application/json\" }", "body: JSON.stringify(" + (op.request.example || "{}").trim() + ")");
    return "const response = await fetch(\"" + host + op.path + "\", {\n  " + init.join(",\n  ") + "\n});\nconst body = await response.json();";
  }

  function port(service) {
    const found = catalog.services.find((item) => item.id === service);
    return found ? found.port : 8080;
  }

  function markdown(parent, source) {
    const lines = source.split("\n");
    let i = 0;
    while (i < lines.length) {
      const line = lines[i];
      if (!line.trim()) { i++; continue; }
      if (line.startsWith("```")) {
        const lang = line.slice(3).trim();
        const buf = [];
        i++;
        while (i < lines.length && !lines[i].startsWith("```")) buf.push(lines[i++]);
        i++;
        labeledCode(parent, lang || "code", buf.join("\n"), lang || "text");
        continue;
      }
      if (line.startsWith(":::flow")) {
        const flow = el("div", "flow");
        i++;
        while (i < lines.length && lines[i].trim() !== ":::") {
          if (lines[i].trim()) flow.append(flowRow(lines[i].split("→").map((part) => part.trim())));
          i++;
        }
        i++;
        parent.append(flow);
        continue;
      }
      if (line.startsWith("|")) {
        const rows = [];
        while (i < lines.length && lines[i].startsWith("|")) rows.push(lines[i++]);
        parent.append(markdownTable(rows));
        continue;
      }
      if (line.startsWith("## ")) { parent.append(el("h2", null, line.slice(3).trim())); i++; continue; }
      if (line.startsWith("### ")) { parent.append(el("h3", null, line.slice(4).trim())); i++; continue; }
      if (line.startsWith("- ")) {
        const list = document.createElement("ul");
        while (i < lines.length && lines[i].startsWith("- ")) {
          const item = document.createElement("li");
          inline(item, lines[i].slice(2));
          list.append(item);
          i++;
        }
        parent.append(list);
        continue;
      }
      const buf = [line];
      i++;
      while (i < lines.length && lines[i].trim() && !lines[i].startsWith("#") && !lines[i].startsWith("|") && !lines[i].startsWith("```") && !lines[i].startsWith(":::") && !lines[i].startsWith("- "))
        buf.push(lines[i++]);
      const paragraph = document.createElement("p");
      inline(paragraph, buf.join(" "));
      parent.append(paragraph);
    }
  }

  function inline(parent, text) {
    const pattern = /(`[^`]+`|\*\*[^*]+\*\*|\[[^\]]+\]\([^)]+\))/g;
    let last = 0;
    for (const match of text.matchAll(pattern)) {
      if (match.index > last) parent.append(document.createTextNode(text.slice(last, match.index)));
      const token = match[0];
      if (token.startsWith("`")) parent.append(code(token.slice(1, -1)));
      else if (token.startsWith("**")) {
        const strong = document.createElement("strong");
        strong.textContent = token.slice(2, -2);
        parent.append(strong);
      } else {
        const label = token.slice(1, token.indexOf("]"));
        const href = token.slice(token.indexOf("(") + 1, -1);
        if (href.startsWith("/") || href.startsWith("http://") || href.startsWith("https://")) {
          const link = document.createElement("a");
          link.href = href;
          link.textContent = label;
          if (href.startsWith("http")) link.target = "_blank";
          parent.append(link);
        } else parent.append(code(href));
      }
      last = match.index + token.length;
    }
    if (last < text.length) parent.append(document.createTextNode(text.slice(last)));
  }

  function markdownTable(rows) {
    const table = document.createElement("table");
    rows.forEach((line, index) => {
      if (index === 1 && /^\|[-| :]+\|$/.test(line.trim())) return;
      const cells = line.split("|").slice(1, -1).map((cell) => cell.trim());
      const tr = document.createElement("tr");
      for (const value of cells) {
        const node = document.createElement(index === 0 ? "th" : "td");
        inline(node, value);
        tr.append(node);
      }
      table.append(tr);
    });
    return table;
  }

  function schemaBlock(node, schema) {
    if (schema.fields && schema.fields.length) node.append(fieldTable(schema.fields));
    if (schema.example) labeledCode(node, schema.record || "Example", schema.example, "json");
  }

  function fieldTable(fields) {
    const table = document.createElement("table");
    table.append(row(["Field", "Type", "Required", "Description"], true));
    for (const field of fields) {
      const tr = document.createElement("tr");
      tr.append(cell(field.name), cell(field.type), cell(field.required ? "yes" : "no"), cell(field.description || ""));
      table.append(tr);
    }
    return table;
  }

  function paramTable(params, headers) {
    const table = document.createElement("table");
    table.append(row([headers ? "Header" : "Name", "Type", "Required", "Default", "Description"], true));
    for (const param of params) {
      const tr = document.createElement("tr");
      tr.append(cell(param.name), cell(param.type), cell(param.required ? "yes" : "no"), cell(param.defaultValue || ""), cell(param.description || ""));
      table.append(tr);
    }
    return table;
  }

  function labeledCode(parent, label, source, lang) {
    const block = el("div", "block");
    const pre = document.createElement("pre");
    const code = document.createElement("code");
    if (lang === "json") code.innerHTML = highlight(source.trim());
    else code.textContent = source.trim();
    pre.append(code);
    const button = el("button", "copy");
    button.type = "button";
    button.textContent = "Copy";
    button.addEventListener("click", () => navigator.clipboard.writeText(source.trim()).then(() => { button.textContent = "Copied"; setTimeout(() => { button.textContent = "Copy"; }, 1200); }));
    block.append(pre, button);
    parent.append(block);
  }

  function highlight(source) {
    return source
      .replace(/&/g, "&amp;").replace(/</g, "&lt;")
      .replace(/(&quot;|")([^"\\]|\\.)*(&quot;|")(?=\s*:)/g, '<span class="token-key">$&</span>')
      .replace(/:\s*(&quot;|")([^"\\]|\\.)*(&quot;|")/g, (match) => match.replace(/(&quot;|")([^"\\]|\\.)*(&quot;|")/, '<span class="token-str">$&</span>'))
      .replace(/\b-?\d+(\.\d+)?\b/g, '<span class="token-num">$&</span>');
  }

  function pretty(body) {
    try { return JSON.stringify(JSON.parse(body), null, 2); } catch { return body; }
  }

  function showSearch(value) {
    const needle = value.trim().toLowerCase();
    results.replaceChildren();
    if (!needle || !catalog) { results.hidden = true; return; }
    const hits = catalog.search.filter((hit) => (hit.title + " " + hit.detail + " " + hit.kind).toLowerCase().includes(needle)).slice(0, 12);
    if (!hits.length) {
      const empty = el("a");
      empty.textContent = "No matches";
      results.append(empty);
    }
    hits.forEach((hit) => {
      const link = document.createElement("a");
      link.href = hit.href;
      const title = document.createElement("span");
      title.textContent = hit.title;
      const detail = document.createElement("small");
      detail.textContent = hit.kind + " · " + hit.detail;
      link.append(title, detail);
      results.append(link);
    });
    activeHit = 0;
    mark([...results.querySelectorAll("a")]);
    results.hidden = false;
  }

  function mark(links) {
    links.forEach((link, index) => link.setAttribute("aria-selected", index === activeHit ? "true" : "false"));
  }

  function hideSearch() { results.hidden = true; }

  function outline() {
    const heads = [...main.querySelectorAll("h2, h3")];
    toc.hidden = heads.length < 2;
    tocNav.replaceChildren();
    heads.forEach((head, index) => {
      if (!head.id) head.id = slug(head.textContent) || "section-" + index;
      const link = document.createElement("a");
      link.href = "#" + head.id;
      link.textContent = head.textContent;
      if (head.tagName === "H3") link.style.paddingLeft = "16px";
      link.addEventListener("click", (event) => {
        event.preventDefault();
        head.scrollIntoView({ behavior: "smooth", block: "start" });
        history.replaceState(null, "", location.pathname + "#" + head.id);
      });
      tocNav.append(link);
    });
    document.getElementById("docs-main").focus({ preventScroll: true });
  }

  function markCurrent(path) {
    nav.querySelectorAll("a").forEach((link) => {
      const current = link.pathname === path;
      if (current) link.setAttribute("aria-current", "page");
      else link.removeAttribute("aria-current");
      if (current) link.closest("details").open = true;
    });
  }

  function missing() {
    heading("Not in the catalog", "This address is not a page the catalog knows.");
    outline();
  }

  function crumbs(group, title) {
    const node = el("p", "crumbs");
    const root = document.createElement("a");
    root.href = "/docs/overview";
    root.textContent = "Docs";
    node.append(root, document.createTextNode(" / " + group + " / " + title));
    main.append(node);
  }

  function heading(title, summary) {
    const h = el("h1", null, title);
    const lede = el("p", "lede", summary || "");
    main.append(h, lede);
    document.title = title + " · ZeroShift docs";
    return h;
  }

  function section(title, fill) {
    const h = el("h2", null, title);
    const body = document.createElement("div");
    fill(body);
    main.append(h, body);
  }

  function flowRow(nodes) {
    const row = el("div", "flow-row");
    nodes.forEach((label, index) => {
      if (index) row.append(el("i", null, "→"));
      const chip = document.createElement("b");
      const href = flowHref(label);
      if (!href) chip.textContent = label;
      else {
        const link = document.createElement("a");
        link.href = href;
        link.textContent = label;
        chip.append(link);
      }
      row.append(chip);
    });
    return row;
  }

  function flowHref(label) {
    const service = catalog.services.find((item) => item.id === label || item.name === label);
    if (service) return "/docs/services/" + service.id;
    const op = catalog.operations.find((item) => item.method + " " + item.path === label || item.path === label);
    if (op) return "/docs/api/" + op.id;
    const topic = catalog.topics.find((item) => item.name === label);
    if (topic) return "/docs/kafka/topics#" + slug(topic.name);
    return null;
  }

  function addFact(table, name, value) {
    const tr = document.createElement("tr");
    tr.append(cell(name), cell(value));
    table.append(tr);
  }

  function row(values, head) {
    const tr = document.createElement("tr");
    for (const value of values) tr.append(cell(value, head));
    return tr;
  }

  function cell(value, head) {
    const node = document.createElement(head ? "th" : "td");
    node.textContent = value;
    return node;
  }

  function cellLink(label, href) {
    const node = document.createElement("td");
    const link = document.createElement("a");
    link.href = href;
    link.textContent = label;
    node.append(link);
    return node;
  }

  function labeled(text, control, name) {
    const label = document.createElement("label");
    label.textContent = text;
    control.name = name;
    label.append(control);
    return label;
  }

  function input(name, value) {
    const node = document.createElement("input");
    node.name = name;
    node.value = value;
    return node;
  }

  function select(values) {
    const node = document.createElement("select");
    for (const value of values) {
      const option = document.createElement("option");
      option.value = value;
      option.textContent = value;
      node.append(option);
    }
    return node;
  }

  function badge(method) {
    const node = el("span", "method " + method.toLowerCase());
    node.textContent = method;
    return node;
  }

  function code(value) {
    const node = document.createElement("code");
    node.textContent = value;
    return node;
  }

  function text(value) {
    const node = document.createElement("p");
    node.textContent = value;
    return node;
  }

  function el(tag, className, textContent) {
    const node = document.createElement(tag);
    if (className) node.className = className;
    if (textContent) node.textContent = textContent;
    return node;
  }

  function slug(value) {
    return value.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "");
  }
})();
