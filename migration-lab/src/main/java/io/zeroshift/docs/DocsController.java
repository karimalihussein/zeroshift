package io.zeroshift.docs;

import io.zeroshift.docs.Model.Portal;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@Configuration
public class DocsController {
  @Bean
  Portal portal() {
    return PortalCatalog.build();
  }
}

@Controller
class DocsPages {
  @GetMapping({"/docs", "/docs/{section}", "/docs/{section}/{page}"})
  public String docs() {
    return "docs";
  }
}

@RestController
class DocsApi {
  private final Portal portal;
  private final DocsProxy proxy;

  DocsApi(Portal portal, DocsProxy proxy) {
    this.portal = portal;
    this.proxy = proxy;
  }

  @GetMapping("/api/docs/catalog")
  Portal catalog() {
    return portal;
  }

  @PostMapping("/api/docs/try")
  DocsProxy.TryResult tryIt(@RequestBody DocsProxy.TryCall call, HttpServletRequest request) {
    var origin =
        request.getScheme() + "://" + request.getServerName() + ":" + request.getServerPort();
    return proxy.execute(call, origin);
  }
}
