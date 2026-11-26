package io.glueops.ipagg.emit;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import io.glueops.ipagg.views.WindowViews;

/** Debug view of the last window this instance pushed: same numbers as the series, without Prometheus. */
@RestController
public class ViewsController {

    private final WindowEmitter emitter;

    public ViewsController(WindowEmitter emitter) {
        this.emitter = emitter;
    }

    @GetMapping("/views/latest")
    public ResponseEntity<WindowViews> latest() {
        WindowViews views = emitter.latest();
        return views == null ? ResponseEntity.noContent().build() : ResponseEntity.ok(views);
    }
}
