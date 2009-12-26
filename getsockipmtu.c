#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>

#define IP_MTU 14

int getsockipmtu(int sockfd)
{
	int ret;
	unsigned int mtu;
	socklen_t len = sizeof(mtu);

	ret = getsockopt(sockfd, SOL_IP, IP_MTU, &mtu, &len);

	return ret < 0 ? -1 : mtu;
}

// Test program

#include <stdio.h>
#include <err.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <errno.h>

static void die(const char *error, ...)
{
	va_list param;

	va_start(param, error);
	verrx(EXIT_FAILURE, error, param);
	va_end(param);
}

static char *host = "127.0.0.1";
static char *port = "80";

static void parse_options(int argc, char **argv)
{
	int c;

	while ((c = getopt(argc, argv, "h:p:")) != -1) {
		switch(c) {
		case 'h':
			host = optarg;
			break;
		case 'p':
			port = optarg;
			break;
		default:
			die("Invalid option %c", c);
			break;
		}
	}
}

/*
 * git/connect.c
 */
static int tcp_connect_sock(char *host, char *port)
{
	int sockfd;
	int saved_errno;
	struct addrinfo hints, *ai0, *ai;
	int ret;

	memset(&hints, 0, sizeof(hints));
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_protocol = IPPROTO_TCP;

	ret = getaddrinfo(host, port, &hints, &ai);
	if (ret) {
		die("Unable to look up %s (port %s) (%s)",
			host, port, gai_strerror(ret));
	}

	for (ai0 = ai; ai; ai = ai->ai_next) {
		sockfd = socket(ai->ai_family,
				ai->ai_socktype, ai->ai_protocol);
		if (sockfd < 0) {
			saved_errno = errno;
			continue;
		}
		ret = connect(sockfd, ai->ai_addr, ai->ai_addrlen);
		if (ret < 0) {
			saved_errno = errno;
			close(sockfd);
			sockfd = -1;
			continue;
		}
		break;
	}

	freeaddrinfo(ai0);
	if (sockfd < 0)
		die("unable to connect a socket (%s)", strerror(saved_errno));

	return sockfd;
}


int main(int argc, char **argv)
{
	int sockfd;
	int mtu;

	parse_options(argc, argv);
	sockfd = tcp_connect_sock(host, port);

	mtu = getsockipmtu(sockfd);
	if (mtu < 0)
		die("unable to get mtu (%s)", strerror(errno));

	printf("%d\n", mtu);

	return 0;
}
