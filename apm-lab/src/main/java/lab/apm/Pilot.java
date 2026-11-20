package lab.apm;

import com.example.doktoribackend.user.policy.PolicyController;
import com.example.doktoribackend.user.policy.PolicyService;
import com.example.doktoribackend.config.MdcTraceFilter;
import jakarta.servlet.*;
import jakarta.servlet.http.*;
import java.io.IOException;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.web.filter.OncePerRequestFilter;

// Separate local executable: imports real application code, no mocks or cloud clients.
@SpringBootApplication
@EntityScan({"com.example.doktoribackend.user.policy", "com.example.doktoribackend.reading.domain"})
@EnableJpaRepositories(basePackages={"com.example.doktoribackend.user.repository", "com.example.doktoribackend.reading.repository"},
 includeFilters=@org.springframework.context.annotation.ComponentScan.Filter(type=org.springframework.context.annotation.FilterType.REGEX,
 pattern=".*Reading(Genre|Purpose|Volume)Repository"))
@Import({PolicyController.class, PolicyService.class})
public class Pilot {
 public static void main(String[] args) { SpringApplication.run(Pilot.class, args); }
 @Bean MdcTraceFilter legacyRequestId() { return new MdcTraceFilter(); }
 @Bean OncePerRequestFilter requestLog() {
  return new OncePerRequestFilter() {
   protected void doFilterInternal(HttpServletRequest req, HttpServletResponse res, FilterChain chain)
     throws ServletException, IOException {
    // Scope the executable to the selected public read endpoint and health check.
    if (!req.getRequestURI().equals("/api/policies/reading-genres") && !req.getRequestURI().equals("/api/actuator/health")) {
     res.sendError(404); return;
    }
    try { chain.doFilter(req,res); }
    finally { LoggerFactory.getLogger("apm.request").info("request_complete method={} path={} status={}",
      req.getMethod(), req.getRequestURI(), res.getStatus()); }
   }
  };
 }
}
