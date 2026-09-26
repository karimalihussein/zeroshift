package io.zeroshift.racelab.web;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;

/** The lab's page. Its template and assets are served by the control plane with the other labs. */
@Controller
public class RaceLabPage {
  private final RaceLabSettings settings;

  public RaceLabPage(RaceLabSettings settings) {
    this.settings = settings;
  }

  @GetMapping("/race")
  public String page(Model model) {
    model.addAttribute("grafanaUrl", settings.grafanaUrl());
    return "race";
  }
}
