package io.github.mgeladzerezo.auction.auth;

import io.github.mgeladzerezo.auction.web.ApiException;
import org.springframework.core.MethodParameter;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.support.WebDataBinderFactory;
import org.springframework.web.context.request.NativeWebRequest;
import org.springframework.web.method.support.HandlerMethodArgumentResolver;
import org.springframework.web.method.support.ModelAndViewContainer;

/**
 * Lets a controller method require authentication simply by declaring a {@link User}
 * parameter. The user comes from the {@code Authorization: Bearer} header; a missing or
 * invalid token is a 401.
 */
@Component
public class CurrentUserResolver implements HandlerMethodArgumentResolver {

    private static final String BEARER = "Bearer ";

    private final AuthService auth;

    public CurrentUserResolver(AuthService auth) {
        this.auth = auth;
    }

    @Override
    public boolean supportsParameter(MethodParameter parameter) {
        return parameter.getParameterType() == User.class;
    }

    @Override
    public User resolveArgument(MethodParameter parameter, ModelAndViewContainer mavContainer,
                                NativeWebRequest webRequest, WebDataBinderFactory binderFactory) {
        String header = webRequest.getHeader(HttpHeaders.AUTHORIZATION);
        if (header == null || !header.regionMatches(true, 0, BEARER, 0, BEARER.length())) {
            throw ApiException.unauthorized();
        }
        return auth.authenticate(header.substring(BEARER.length()).trim()).orElseThrow(ApiException::unauthorized);
    }
}
